package info.batey.kafka.unit.TestSink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestSinkTask extends SinkTask {

    public static List<SinkRecord> sinkRecordList = new ArrayList<>();

    public String version() {
        return "1.0";
    }

    public void start(Map<String, String> configProps) {
    }

    public void put(Collection<SinkRecord> sinkRecords) {
        sinkRecordList.addAll(sinkRecords);
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    public void stop() {
        sinkRecordList.clear();
    }
}