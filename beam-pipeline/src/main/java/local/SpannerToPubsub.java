package local;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToPubsub {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerToPubsub.class);
    private static final Gson GSON = new Gson();
    private static final String OUTBOX_TABLE = "Outbox";
    private static final String PAYLOAD_FIELD = "Payload";

    public interface Options extends PipelineOptions {
        @Description("GCP project ID (used by Spanner emulator)")
        @Validation.Required
        String getProjectId();
        void setProjectId(String value);

        @Description("Spanner instance ID")
        @Validation.Required
        String getInstanceId();
        void setInstanceId(String value);

        @Description("Spanner database ID")
        @Validation.Required
        String getDatabaseId();
        void setDatabaseId(String value);

        @Description("Change stream name")
        @Validation.Required
        String getChangeStreamName();
        void setChangeStreamName(String value);

        @Description("Pub/Sub topic (projects/<project>/topics/<topic>)")
        @Validation.Required
        String getPubsubTopic();
        void setPubsubTopic(String value);

        @Description("Spanner emulator host (host:port)")
        String getSpannerEmulatorHost();
        void setSpannerEmulatorHost(String value);

        @Description("Change stream inclusive start offset seconds (defaults to 60s)")
        Integer getStartOffsetSeconds();
        void setStartOffsetSeconds(Integer value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        SpannerConfig spannerConfig = SpannerConfig.create()
            .withProjectId(options.getProjectId())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());

        if (options.getSpannerEmulatorHost() != null && !options.getSpannerEmulatorHost().isEmpty()) {
            spannerConfig = spannerConfig.withEmulatorHost(
                ValueProvider.StaticValueProvider.of(options.getSpannerEmulatorHost()));
        }

        Pipeline pipeline = Pipeline.create(options);

        int startOffsetSeconds = options.getStartOffsetSeconds() == null ? 60 : options.getStartOffsetSeconds();
        Timestamp now = Timestamp.now();
        long startSeconds = now.getSeconds() - startOffsetSeconds - 1;
        if (startSeconds < 0) {
            startSeconds = 0;
        }
        Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(startSeconds, 0);
        LOG.info("changeStreamStartTimestamp={} (offsetSeconds={})", startTimestamp.toString(), startOffsetSeconds);

        PCollection<DataChangeRecord> records = pipeline.apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(spannerConfig)
                .withChangeStreamName(options.getChangeStreamName())
                .withMetadataInstance(options.getInstanceId())
                .withMetadataDatabase(options.getDatabaseId())
                .withInclusiveStartAt(startTimestamp)
        );

        PCollection<byte[]> payloads = records.apply(
            "ExtractOutboxPayloads",
            ParDo.of(new ExtractOutboxPayloadFn()));

        payloads
            .apply("PublishGrpc", ParDo.of(new PubsubPublishFn(
                options.getProjectId(),
                options.getPubsubTopic())));

        pipeline.run().waitUntilFinish();
    }

    private static String extractTopicId(String topic) {
        int idx = topic.lastIndexOf("/topics/");
        if (idx >= 0) {
            return topic.substring(idx + "/topics/".length());
        }
        return topic;
    }

    private static byte[] decodePayload(String payloadValue) {
        if (payloadValue == null) {
            return new byte[0];
        }
        try {
            return Base64.getDecoder().decode(payloadValue);
        } catch (IllegalArgumentException ex) {
            return payloadValue.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final class ExtractOutboxPayloadFn extends DoFn<DataChangeRecord, byte[]> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            DataChangeRecord record = context.element();
            if (record == null || record.getTableName() == null) {
                return;
            }
            if (!OUTBOX_TABLE.equalsIgnoreCase(record.getTableName())) {
                return;
            }
            List<Mod> mods = record.getMods();
            if (mods == null || mods.isEmpty()) {
                return;
            }
            for (Mod mod : mods) {
                String newValuesJson = mod == null ? null : mod.getNewValuesJson();
                if (newValuesJson == null || newValuesJson.isEmpty()) {
                    continue;
                }
                JsonObject obj = GSON.fromJson(newValuesJson, JsonObject.class);
                if (obj == null) {
                    continue;
                }
                JsonElement payloadElement = getPayloadElement(obj);
                if (payloadElement == null || payloadElement.isJsonNull()) {
                    continue;
                }
                byte[] payload = decodePayload(payloadElement.getAsString());
                if (payload.length == 0) {
                    continue;
                }
                LOG.info("outbox payload emitted bytes={}", payload.length);
                context.output(payload);
            }
        }
    }

    private static JsonElement getPayloadElement(JsonObject obj) {
        JsonElement direct = obj.get(PAYLOAD_FIELD);
        if (direct != null) {
            return direct;
        }
        for (String key : obj.keySet()) {
            if (PAYLOAD_FIELD.equalsIgnoreCase(key)) {
                return obj.get(key);
            }
        }
        return null;
    }

    private static final class PubsubPublishFn extends DoFn<byte[], Void> {
        private final String projectId;
        private final String topic;
        private transient Publisher publisher;
        private transient ManagedChannel channel;

        private PubsubPublishFn(String projectId, String topic) {
            this.projectId = projectId;
            this.topic = topic;
        }

        @Setup
        public void setup() throws Exception {
            String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
            if (emulatorHost == null || emulatorHost.isEmpty()) {
                throw new IllegalStateException("PUBSUB_EMULATOR_HOST is required for gRPC publish");
            }

            String topicId = extractTopicId(topic);
            channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
            Publisher.Builder builder = Publisher.newBuilder(TopicName.of(projectId, topicId))
                .setChannelProvider(FixedTransportChannelProvider.create(
                    GrpcTransportChannel.create(channel)))
                .setCredentialsProvider(NoCredentialsProvider.create());
            publisher = builder.build();
            LOG.info("gRPC publisher configured: emulatorHost={} topicId={}", emulatorHost, topicId);
        }

        @Teardown
        public void teardown() throws Exception {
            if (publisher != null) {
                publisher.shutdown();
            }
            if (channel != null) {
                channel.shutdown();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(context.element()))
                .build();
            ApiFuture<String> future = publisher.publish(message);
            future.get();
        }
    }
}
