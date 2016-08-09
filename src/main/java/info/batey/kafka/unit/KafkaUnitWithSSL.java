package info.batey.kafka.unit;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_HOST_NAME;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_ID;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_INTER_BROKER_PROTOCOL;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_LISTENERS;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_PORT;
import static info.batey.kafka.unit.KafkaUnitConfig.CLIENT_SECURITY_PROTOCOL;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_CONNECT;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_LOG_DIRECTORY;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_CLIENT_AUTH_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;

public class KafkaUnitWithSSL extends AbstractKafkaUnit {

    public KafkaUnitWithSSL(int zkPort, int brokerPort) {
        super();
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.certStorePath = getLocalCertStorePath();
        this.zookeeperUri = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    public KafkaUnitWithSSL(int zkPort, int brokerPort, String certStorePath) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.certStorePath = certStorePath;
        this.zookeeperUri = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    @Override Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
        getSSLClientConfig(props);
        return props;
    }

    @Override void setBrokerConfig() {
        final File logDir = getLogDirectory();
        kafkaBrokerConfig.setProperty(ZOOKEEPER_CONNECT, zookeeperUri);
        kafkaBrokerConfig.setProperty(BROKER_ID, "1");
        kafkaBrokerConfig.setProperty(BROKER_HOST_NAME, "localhost");
        kafkaBrokerConfig.setProperty(BROKER_PORT, Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_DIRECTORY, logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES, valueOf(1));
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, certStorePath + "/server.keystore.jks");
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_LOCATION_CONFIG, certStorePath + "/server.truststore.jks");
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_KEY_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_CLIENT_AUTH_CONFIG, "required");
        kafkaBrokerConfig.setProperty(BROKER_LISTENERS, format("SSL://localhost:%d", brokerPort));
        kafkaBrokerConfig.setProperty(BROKER_INTER_BROKER_PROTOCOL, "SSL");
    }

    public ConsumerRecords<String, String> readMessages(String topicName, final int expectedMessages) {
        try (KafkaConsumer consumer = getNewConsumer();) {
            consumer.subscribe(Arrays.asList(topicName));
            final ConsumerRecords<String, String> records = consumer.poll(timeout_3_Seconds);
            if (records.count() != expectedMessages) {
                throw new ComparisonFailure("Incorrect number of messages returned",
                    Integer.toString(expectedMessages),
                    Integer.toString(records.count())
                );
            }
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Received message: {}", record.value());
            }
            return records;
        }
    }

    private KafkaConsumer getNewConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", brokerPort));
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(GROUP_ID_CONFIG, "10");
        getSSLClientConfig(props);
        return new KafkaConsumer(props);
    }

    private void getSSLClientConfig(Properties props) {
        props.put(CLIENT_SECURITY_PROTOCOL, "SSL");
        props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, certStorePath + "/client.truststore.jks");
        props.put(SSL_KEYSTORE_LOCATION_CONFIG, certStorePath + "/client.keystore.jks");
        props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        props.put(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
    }

    private String getLocalCertStorePath() {
        final URL resource = this.getClass().getResource("/certStore");
        return resource.getPath();
    }
}
