package com.hackathon.services.kinesisanalytics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class FlowLogStreamingJob {
    private static final Log LOG = LogFactory.getLog(FlowLogStreamingJob.class);
    private static final String REGION_NAME = "us-east-1";
    private static final String INPUT_DATA_STREAM_NAME = "vpc-flow-logs-extraction-data-stream";
    private static final String S3_SINK_PATH = "s3a://st6-vpc-flow-perf-test-kda-compressed/";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int envParallelism = env.getParallelism();
        DataStream<EventData> data = env.addSource(getSourceFromStaticConfig()).name("KinesisSource").uid("KinesisSource")
                .flatMap(new FlowLogMap()).name("FlowLogMap").uid("FlowLogMap");
        data.addSink(createS3SinkFromStaticConfig()).name("s3Compressed").uid("s3Compressed").setParallelism(getParallelismLimit(envParallelism, 10));

        env.execute("VPCFlowLog streaming job");
    }

    static int getParallelismLimit(int total, int percentage) {
        return (int) Math.round(percentage / 100.0 * total);
    }


    private static FlinkKinesisConsumer getSourceFromStaticConfig() {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, REGION_NAME);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        return new FlinkKinesisConsumer<>(
                INPUT_DATA_STREAM_NAME,
                new SimpleStringSchema(),
                inputProperties);
    }

    private static SinkFunction<EventData> createS3SinkFromStaticConfig() {
        return StreamingFileSink
                .forBulkFormat(new Path(S3_SINK_PATH), CustomParquetAvroWriters.forSpecificRecord(EventData.class))
                .withBucketAssigner(new S3FolderBucketAssigner())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".parquet")
                        .build())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }
}