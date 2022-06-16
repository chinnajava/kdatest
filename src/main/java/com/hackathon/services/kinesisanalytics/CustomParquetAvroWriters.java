package com.hackathon.services.kinesisanalytics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

public class CustomParquetAvroWriters {
    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(Class<T> type) {
        String schemaString = SpecificData.get().getSchema(type).toString();
        ParquetBuilder<T> builder = (out) -> {
            return createAvroParquetWriter(schemaString, SpecificData.get(), out);
        };
        return new ParquetWriterFactory(builder);
    }

    private static <T> ParquetWriter<T> createAvroParquetWriter(String schemaString, GenericData dataModel, OutputFile out) throws IOException {
        Schema schema = (new Schema.Parser()).parse(schemaString);
        return (ParquetWriter<T>) AvroParquetWriter.builder(out).withSchema(schema).withDataModel(dataModel).withCompressionCodec(CompressionCodecName.SNAPPY).build();
    }

}
