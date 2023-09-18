package ifrat.com.flink.examples.otel;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class ExportMetricsServiceRequestUtil {

    public static long extractTimestamp(ExportMetricsServiceRequest request) {
        List<ResourceMetrics> resourceMetrics = request.getResourceMetricsList();

        long currentTimestamp = System.currentTimeMillis();
        for (ResourceMetrics metrics : resourceMetrics) {
            Resource resource = metrics.getResource();
            List<KeyValue> keyValueList = resource.getAttributesList();
            for (KeyValue keyValue : keyValueList) {
                String key = keyValue.getKey();
                AnyValue anyValue = keyValue.getValue();
                String stringValue = anyValue.getStringValue();
                if ("timestamp".equals(key)) {
                    currentTimestamp = Long.parseLong(stringValue);
                }
            }
        }

        return currentTimestamp;
    }

    public static List<UnionMetricGroupByModel> extractMetric(ExportMetricsServiceRequest value) {
        List<ResourceMetrics> resourceMetrics = value.getResourceMetricsList();
        final long timestamp = ExportMetricsServiceRequestUtil.extractTimestamp(value);
        List<UnionMetricGroupByModel> results = new LinkedList<>();
        // sw_jvm.cpu.usage and sw_jvm.thread.live.count 这两个指标
        for (ResourceMetrics metrics : resourceMetrics) {
            List<ScopeMetrics> scopeMetrics = metrics.getScopeMetricsList();
            for (ScopeMetrics scopeMetric : scopeMetrics) {
                if (scopeMetric.getMetricsCount() > 0) {
                    for (Metric metric : scopeMetric.getMetricsList()) {
                        // 提取出指标
                        UnionMetricGroupByModel result = new UnionMetricGroupByModel();
                        result.setName(metric.getName());
                        long val = metric.getGauge().getDataPointsCount() > 0 ?
                                metric.getGauge().getDataPoints(0).getAsInt() : 0;
                        result.setValue(val);
                        result.setTimestamp(timestamp);
                        results.add(result);
                    }
                }
            }
        }

        return results;
    }

    public static KafkaSource<ExportMetricsServiceRequest> buildKafkaSource() {

        return KafkaSource.<ExportMetricsServiceRequest>builder()
                .setBootstrapServers("10.253.17.30:39094")
                .setTopics("skywalking-otel-meters")
                .setGroupId("cls-cgroup")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<ExportMetricsServiceRequest>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                                            Collector<ExportMetricsServiceRequest> collector) throws IOException {

                        byte[] value = consumerRecord.value();
                        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(value);

                        collector.collect(request);
                    }

                    @Override
                    public TypeInformation<ExportMetricsServiceRequest> getProducedType() {
                        return TypeInformation.of(ExportMetricsServiceRequest.class);
                    }
                })
                .build();
    }
}
