package com.hackathon.services.kinesisanalytics;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FlowLogMap implements FlatMapFunction<String, EventData> {
    private static final Log LOG = LogFactory.getLog(FlowLogMap.class);
    Set<String> logStatusSet = new HashSet<>(Arrays.asList("nodata", "skipdata"));

    @Override
    public void flatMap(String json, Collector<EventData> collector) {
        try {
            JsonObject data = JsonParser.parseString(json).getAsJsonObject();
            JsonObject event = data.getAsJsonObject("event");
            if (!logStatusSet.contains(event.get("log-status").getAsString().toLowerCase())) {
                JsonObject meta = data.getAsJsonObject("meta");
                EventData eventData = buildEventData(data.get("id").getAsString(), event, meta);
                collector.collect(eventData);
            }
        } catch (Exception e) {
            LOG.error("Exception while parsing data for event : " + json + " : Error " + e);
        }
    }

    private EventData buildEventData(String id, JsonObject event, JsonObject meta) {
        EventData data = new EventData();
        data.setId(id);

        // Set fields from event object
        data.setVersion(event.get("version").getAsInt());
        data.setAccountId(event.get("account-id").getAsString());
        data.setInterfaceId(event.get("interface-id").getAsString());
        data.setSrcaddr(event.get("srcaddr").getAsString());
        data.setDstaddr(event.get("dstaddr").getAsString());
        data.setSrcport(event.get("srcport").getAsString());
        data.setDstport(event.get("dstport").getAsString());
        data.setProtocol(event.get("protocol").getAsInt());
        data.setPackets(event.get("packets").getAsInt());
        data.setBytes(event.get("bytes").getAsLong());
        data.setStart(event.get("start").getAsInt());
        data.setEnd(event.get("end").getAsInt());
        data.setAction(event.get("action").getAsString());
        data.setLogStatus(event.get("log-status").getAsString());
        data.setVpcId(event.get("vpc-id").getAsString());
        data.setSubnetId(event.get("subnet-id").getAsString());
        data.setInstanceId(event.get("instance-id").getAsString());
        data.setTcpFlags(event.get("tcp-flags").getAsInt());
        data.setType(event.get("type").getAsString());
        data.setPktSrcaddr(event.get("pkt-srcaddr").getAsString());
        data.setPktDstaddr(event.get("pkt-dstaddr").getAsString());

        // Set fields from meta object
        data.setMetaS3File(meta.get("meta_s3_file").getAsString());
        data.setMetaAccountNo(meta.get("meta_account_no").getAsString());
        data.setMetaRegion(meta.get("meta_region").getAsString());
        data.setMetaAccountName(meta.get("meta_account_name").getAsString());
        return data;
    }
}
