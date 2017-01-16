package info.batey.kafka.unit;

import info.batey.kafka.unit.TestSink.TestSinkTask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConnectUnitTest {

    private Map<String, String> workerProperties;

    private KafkaUnit kafkaUnit;

    @Before
    public void setUp() throws IOException {
        ConsoleAppender console = new ConsoleAppender();
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.ERROR);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);

        kafkaUnit = new KafkaUnit();
        kafkaUnit.startup();
        kafkaUnit.createTopic("test");

        File offsetFile = File.createTempFile("kafka", "connect");
        offsetFile.deleteOnExit();

        workerProperties = new HashMap<>();
        workerProperties.put("bootstrap.servers", kafkaUnit.getKafkaConnect());
        workerProperties.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        workerProperties.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        workerProperties.put("internal.key.converter", "org.apache.kafka.connect.storage.StringConverter");
        workerProperties.put("internal.value.converter", "org.apache.kafka.connect.storage.StringConverter");
        workerProperties.put("offset.storage.file.filename", offsetFile.getAbsolutePath());
    }

    @After
    public void tearDown() {
        kafkaUnit.shutdown();
    }

    @Test
    public void successfullyConstructTestSinkAndDeliverMessage() throws Exception {
        Map<String, String> connectorProperties = new HashMap<>();

        connectorProperties.put("name", "TestSync");
        connectorProperties.put("connector.class", "info.batey.kafka.unit.TestSink.TestSinkConnector");
        connectorProperties.put("topics", "test");
        connectorProperties.put("max.tasks", "1");
        connectorProperties.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorProperties.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnit.getKafkaConnect());
        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "msg1", "testmessage");
        producer.send(record);

        KafkaConnect kc = new KafkaConnect(workerProperties, connectorProperties);

        // Not great, but need to wait for things to flush through
        Thread.sleep(5000);

        assert (TestSinkTask.sinkRecordList.size() == 1);

        SinkRecord r = TestSinkTask.sinkRecordList.get(0);
        assert (r.key().equals("msg1"));
        assert (r.value().equals("testmessage"));

        kc.shutdown();
    }
}
