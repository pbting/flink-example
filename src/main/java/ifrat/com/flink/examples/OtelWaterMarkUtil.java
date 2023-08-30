package ifrat.com.flink.examples;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public final class OtelWaterMarkUtil {

    public static WatermarkStrategy<ExportMetricsServiceRequest> otelWatermark() {

        return WatermarkStrategy.<ExportMetricsServiceRequest>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<ExportMetricsServiceRequest>() {
                    @Override
                    public long extractTimestamp(ExportMetricsServiceRequest element, long recordTimestamp) {

                        return ExportMetricsServiceRequestUtil.extractTimestamp(element);
                    }
                });
    }
}
