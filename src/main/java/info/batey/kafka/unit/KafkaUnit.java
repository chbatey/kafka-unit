/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.batey.kafka.unit;

import kafka.admin.TopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

public class KafkaUnit {

    protected static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);
    protected KafkaServerStartable broker;

    protected Zookeeper zookeeper;
    protected final String zookeeperUri;
    protected final String brokerString;
    protected int zkPort;
    protected int brokerPort;
    protected Optional<String> keystorePath = Optional.empty();
    protected Properties kafkaBrokerConfig = new Properties();
    protected boolean sslEnabled = false;
    protected String certStorePath = "";
    protected int timeout_3_Seconds = 3000;

    public KafkaUnit(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperUri = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    public KafkaUnit(int zkPort, int brokerPort, boolean sslEnabled) {
        this(zkPort, brokerPort);
        this.sslEnabled = sslEnabled;
        this.certStorePath = getCertStorePath();
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }


    private static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }


    public void startup() {
        zookeeper = new Zookeeper(zkPort);

        zookeeper.startup();

        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        }));
        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperUri);
        kafkaBrokerConfig.setProperty("broker.id", "1");
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", valueOf(1));

        if (sslEnabled) {
            setServerSSLProperties();
        }

        final KafkaConfig serverConfig = new KafkaConfig(kafkaBrokerConfig);
        broker = new KafkaServerStartable(serverConfig);
        broker.startup();
    }


    public void shutdown() {

        if (broker != null) {
            broker.shutdown();
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }


    public List<ProducerRecord<String, String>> readProducerRecords(final String topicName, final int expectedMessages)
        throws TimeoutException {
        return readMessages(topicName, expectedMessages, new MessageExtractor<ProducerRecord<String, String>>() {

            @Override
            public ProducerRecord<String, String> extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return new ProducerRecord<>(topicName, messageAndMetadata.key(), messageAndMetadata.message());
            }
        });
    }

    public List<String> readMessages(String topicName, final int expectedMessages) throws TimeoutException {
        return readMessages(topicName, expectedMessages, new MessageExtractor<String>() {
            @Override
            public String extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return messageAndMetadata.message();
            }
        });
    }

    private <T> List<T> readMessages(
        String topicName, final int expectedMessages, final MessageExtractor<T> messageExtractor
    ) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", zookeeperUri);
        consumerProperties.put("socket.timeout.ms", "500");
        consumerProperties.put("consumer.id", "test");
        consumerProperties.put("consumer.timeout.ms", "500");
        consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
        consumerProperties.put(GROUP_ID_CONFIG, "10");
        consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, "smallest");

        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(
            consumerProperties));
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topicName, 1);
        Map<String, List<KafkaStream<String, String>>> events = javaConsumerConnector.createMessageStreams(topicMap,
            stringDecoder,
            stringDecoder
        );
        List<KafkaStream<String, String>> events1 = events.get(topicName);
        final KafkaStream<String, String> kafkaStreams = events1.get(0);


        Future<List<T>> submit = singleThread.submit(new Callable<List<T>>() {
            public List<T> call() throws Exception {
                List<T> messages = new ArrayList<>();
                try {
                    for (MessageAndMetadata<String, String> kafkaStream : kafkaStreams) {
                        T message = messageExtractor.extract(kafkaStream);
                        LOGGER.info("Received message: {}", kafkaStream.message());
                        messages.add(message);
                    }
                } catch (ConsumerTimeoutException e) {
                    // always gets throws reaching the end of the stream
                }
                if (messages.size() != expectedMessages) {
                    throw new ComparisonFailure("Incorrect number of messages returned",
                        Integer.toString(expectedMessages),
                        Integer.toString(messages.size())
                    );
                }
                return messages;
            }
        });

        try {
            return submit.get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e.getCause() instanceof ComparisonFailure) {
                throw (ComparisonFailure) e.getCause();
            }
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            singleThread.shutdown();
            javaConsumerConnector.shutdown();
        }
    }

    public ConsumerRecords<String, String> readMessagesOverSSL(String topicName, final int expectedMessages) {
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

    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
        if (sslEnabled) {
            getSSLClientConfig(props);
        }
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer getNewConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", brokerPort));
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(GROUP_ID_CONFIG, "10");
        if (sslEnabled) {
            getSSLClientConfig(props);
        }
        return new KafkaConsumer(props);
    }

    public void createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperUri;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled()
        );

        // run
        LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.createTopic(zkUtils, opts);
    }


    @SafeVarargs
    public final void sendMessages(ProducerRecord<String, String> message, ProducerRecord<String, String>... messages) {

        try (Producer<String, String> producer = createProducer();) {
            producer.send(message);
            for (ProducerRecord<String, String> msg : messages) {
                producer.send(msg);
            }
        }
    }

    /**
     * Set custom broker configuration.
     * See avaliable config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    private interface MessageExtractor<T> {
        T extract(MessageAndMetadata<String, String> messageAndMetadata);
    }


    private void setServerSSLProperties() {
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, certStorePath + "/server.keystore.jks");
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_LOCATION_CONFIG, certStorePath + "/server.truststore.jks");
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_KEY_PASSWORD_CONFIG, "test1234");
        kafkaBrokerConfig.setProperty(SSL_CLIENT_AUTH_CONFIG, "required");
        kafkaBrokerConfig.setProperty("listeners", format("SSL://localhost:%d", brokerPort));
        kafkaBrokerConfig.setProperty("security.inter.broker.protocol", "SSL");
    }


    private void getSSLClientConfig(Properties props) {
        props.put("security.protocol", "SSL");
        props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, certStorePath + "/client.truststore.jks");
        props.put(SSL_KEYSTORE_LOCATION_CONFIG, certStorePath + "/client.keystore.jks");
        props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        props.put(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
    }

    private String getCertStorePath() {
        final URL resource = this.getClass().getResource("/certStore");
        return resource.getPath();
    }
}

