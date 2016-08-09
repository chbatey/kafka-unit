package info.batey.kafka.unit.ssl;

import info.batey.kafka.unit.KafkaUnitWithSSL;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.junit.Assert.assertEquals;

public class KafkaSSLIntegrationTest  {

    private KafkaUnitWithSSL kafkaUnitServer;
    private static final int ZOOKEEPER_PORT = 6000;
    private static final int BROKER_PORT = 6001;

    Function<ConsumerRecords<String, String>, List<String>> consumerRecordsToList = (cr) -> {
        List<String> expectedMessages = new ArrayList<>();
        cr.forEach(r -> expectedMessages.add(r.value()));
        return expectedMessages;
    };

    @Before
    public void setUp() throws Exception {
        kafkaUnitServer = new KafkaUnitWithSSL(ZOOKEEPER_PORT, BROKER_PORT);
        kafkaUnitServer.setKafkaBrokerConfig("log.segment.bytes", "1024");
        kafkaUnitServer.startup();
    }

    @After
    public void shutdown() throws Exception {
        Field f = kafkaUnitServer.getClass().getSuperclass().getDeclaredField("broker");
        f.setAccessible(true);
        KafkaServerStartable broker = (KafkaServerStartable) f.get(kafkaUnitServer);
        assertEquals(1024, (int)broker.serverConfig().logSegmentBytes());
        kafkaUnitServer.shutdown();
    }

    @Test
    public void kafkaServerIsAvailable() throws Exception {
        assertKafkaServerIsAvailableWithSSL(kafkaUnitServer);
    }

    @Test
    public void canUseKafkaConnectToProduceWithSSL() throws Exception {
        final String topic = "KafkakConnectTestTopic";
        Properties props = getKafkaSSLConfigProperties();
        Producer<Long, String> producer = new KafkaProducer<>(props);
        ProducerRecord<Long, String> record = new ProducerRecord<>(topic, 1L, "test");
        producer.send(record);
        final ConsumerRecords<String, String> expectedRecords = kafkaUnitServer.readMessages(topic, 1);
        assertEquals("test", consumerRecordsToList.apply(expectedRecords).get(0));
    }

    @Test
    public void canReadProducerRecords() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic, "key", "value");

        //when
        kafkaUnitServer.sendMessages(producerRecord);

        ConsumerRecords<String, String> receivedMessages = kafkaUnitServer.readMessages(testTopic, 1);
        ConsumerRecord<String, String> recievedMessage =null;
        if(receivedMessages.iterator().hasNext()){
          recievedMessage = receivedMessages.iterator().next();
        }

        assertEquals("Received message value is incorrect", "value", recievedMessage.value());
        assertEquals("Received message key is incorrect", "key", recievedMessage.key());
        assertEquals("Received message topic is incorrect", testTopic, recievedMessage.topic());
    }

    private Properties getKafkaSSLConfigProperties() {
        String certStorePath = getCertStorePath();
        Properties props = new Properties();
        props.put("security.protocol", "SSL");
        props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, certStorePath + "/client.truststore.jks");
        props.put(SSL_KEYSTORE_LOCATION_CONFIG, certStorePath + "/client.keystore.jks");
        props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        props.put(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect());
        return props;
    }

    private void assertKafkaServerIsAvailableWithSSL(KafkaUnitWithSSL server) throws TimeoutException {
        //given
        String testTopic = "TestTopic";
        List<String> expectedMessages = new ArrayList<>();
        server.createTopic(testTopic);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic, "key", "value");

        //when
        server.sendMessages(producerRecord);
        ConsumerRecords<String, String> messages = server.readMessages(testTopic, 1);
        messages.forEach(r -> expectedMessages.add(r.value()));
        //then
        assertEquals(Arrays.asList("value"),expectedMessages);
    }

    private String getCertStorePath() {
        final URL resource = this.getClass().getResource("/certStore");
        return resource.getPath();
    }
}
