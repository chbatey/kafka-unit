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
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

public class KafkaUnit {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);
    public static final int DEFAULT_TIMEOUT_MS = 8000;

    private KafkaServerStartable broker;

    private Zookeeper zookeeper;
    private final String zookeeperString;
    private final String brokerString;
    private int zkPort;
    private int brokerPort;
    private KafkaProducer<String, String> producer = null;
    private Properties kafkaBrokerConfig = new Properties();
    private int zkMaxConnections;

    public KafkaUnit() throws IOException {
        this(getEphemeralPort(), getEphemeralPort());
    }

    public KafkaUnit(int zkPort, int brokerPort) {
        this(zkPort, brokerPort, 16);
    }

    public KafkaUnit(int zkPort, int brokerPort, int zkMaxConnections) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
        this.zkMaxConnections = zkMaxConnections;
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString, int zkMaxConnections) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString), zkMaxConnections);
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
        zookeeper = new Zookeeper(zkPort, zkMaxConnections);
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
        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
        kafkaBrokerConfig.setProperty("broker.id", "1");
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        kafkaBrokerConfig.setProperty("delete.topic.enable", String.valueOf(true));
        kafkaBrokerConfig.setProperty("offsets.topic.replication.factor", String.valueOf(1));

        broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
        broker.startup();
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

    public void createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperString;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        try {
            // run
            LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
            TopicCommand.createTopic(zkUtils, opts);
        } finally {
            zkUtils.close();
        }

    }

    /**
     * @return All topic names
     */
    public List<String> listTopics() {
        String[] arguments = new String[3];
        arguments[0] = "--zookeeper";
        arguments[1] = zookeeperString;
        arguments[2] = "--list";
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        final List<String> topics = new ArrayList<>();
        try {
            // run
            LOGGER.info("Executing: ListTopics " + Arrays.toString(arguments));

            PrintStream oldOut = Console.out();
            try {
                Console.setOut(new PrintStream(oldOut) {
                    @Override
                    public void print(String s) {
                        super.print(s);
                        if (!s.endsWith("marked for deletion")) {
                            topics.add(s);
                        }
                    }
                });
                TopicCommand.listTopics(zkUtils, opts);
            } finally {
                Console.setOut(oldOut);
            }
        } finally {
            zkUtils.close();
        }

        return topics;
    }

    /**
     * Delete all topics
     */
    public void deleteAllTopics() {
        for (String topic : listTopics()) {
            try {
                deleteTopic(topic);
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Delete a topic.
     *
     * @param topicName The name of the topic to delete
     */
    public void deleteTopic(String topicName) {
        String[] arguments = new String[5];
        arguments[0] = "--zookeeper";
        arguments[1] = zookeeperString;
        arguments[2] = "--delete";
        arguments[3] = "--topic";
        arguments[4] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        try {
            // run
            LOGGER.info("Executing: DeleteTopic " + Arrays.toString(arguments));
            TopicCommand.deleteTopic(zkUtils, opts);
        } finally {
            zkUtils.close();
        }
    }

    public void shutdown() {
        if (broker != null) broker.shutdown();
        if (zookeeper != null) zookeeper.shutdown();
    }

    public List<ConsumerRecord<String, String>> readKeyedMessages(final String topicName, final int expectedMessages, long timeoutMs) throws TimeoutException {
        return readMessages(topicName, expectedMessages, timeoutMs, new MessageExtractor<ConsumerRecord<String, String>>() {
            @Override
            public ConsumerRecord<String, String> extract(ConsumerRecord<String, String> messageAndMetadata) {
                return messageAndMetadata;
            }
        });
    }

    public List<ConsumerRecord<String, String>> readKeyedMessages(final String topicName, final int expectedMessages) throws TimeoutException {
        return this.readKeyedMessages(topicName, expectedMessages, DEFAULT_TIMEOUT_MS);
    }

    public List<String> readMessages(String topicName, final int expectedMessages, long timeoutMs) throws TimeoutException {
        return readMessages(topicName, expectedMessages, timeoutMs, new MessageExtractor<String>() {
            @Override
            public String extract(ConsumerRecord<String, String> messageAndMetadata) {
                return messageAndMetadata.value();
            }
        });
    }

    public List<String> readMessages(String topicName, final int expectedMessages) throws TimeoutException {
        return readMessages(topicName, expectedMessages, DEFAULT_TIMEOUT_MS);
    }

    public List<String> pollMessages(String topicName) throws TimeoutException {
        return readMessages(topicName, -1, DEFAULT_TIMEOUT_MS);
    }

    private <T> List<T> readMessages(final String topicName, final int expectedMessages, final long timeoutMs, final MessageExtractor<T> messageExtractor) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerString);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-unit");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "read");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Future<List<T>> submit = singleThread.submit(new Callable<List<T>>() {
            public List<T> call() throws Exception {
                List<T> messages = new ArrayList<>();
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
                    consumer.subscribe(Collections.singleton(topicName));
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(timeoutMs);
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        T message = messageExtractor.extract(record);
                        LOGGER.info("Received message: {}", record.value());
                        messages.add(message);
                    }
                }
                if (expectedMessages >= 0 && messages.size() != expectedMessages) {
                    throw new ComparisonFailure("Incorrect number of messages returned", Integer.toString(expectedMessages),
                            Integer.toString(messages.size()));
                }
                return messages;
            }
        });

        try {
            return submit.get();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof ComparisonFailure) {
                throw (ComparisonFailure) e.getCause();
            }
            TimeoutException timeoutException = new TimeoutException("Timed out waiting for messages");
            timeoutException.initCause(e);
            throw timeoutException;
        } finally {
            singleThread.shutdown();
        }
    }

    @SafeVarargs
    public final void sendMessages(ProducerRecord<String, String> message, ProducerRecord<String, String>... messages) {
        if (producer == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerString);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-unit-producer");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        }
        try {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            futures.add(producer.send(message));
            for (ProducerRecord<String, String> record : messages) {
                futures.add(producer.send(record));
            }
            // waiting until all messages have been send
            for (Future<RecordMetadata> future : futures) {
                future.get();
            }
            producer.flush();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Failed sending messages", e);
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
        T extract(ConsumerRecord<String, String> messageAndMetadata);
    }
}

