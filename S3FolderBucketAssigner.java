package com.hackathon.services.kinesisanalytics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class S3FolderBucketAssigner implements BucketAssigner<EventData, String>, Serializable {
    private static final Log LOG = LogFactory.getLog(S3FolderBucketAssigner.class);

    @Override
    public String getBucketId(EventData event, Context context) {
        ZonedDateTime zd = Instant.ofEpochSecond(event.getStart()).atZone(ZoneOffset.UTC);
        String folder = String.format("year=%04d/month=%02d/day=%02d/hour=%02d/minute=%02d/",
                zd.getYear(), zd.getMonth().getValue(), zd.getDayOfMonth(), zd.getHour(), zd.getMinute());
        return folder;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}