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

import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.TopicCommand;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaUnit {

    private static final Logger logger = LoggerFactory.getLogger(KafkaUnit.class);

    private List<KafkaBroker> brokers;
    private Zookeeper zookeeper;
    private final String zookeeperString;
    private String brokerString;
    private int zkPort;
    private int brokerPort = -1;
    private KafkaProducer<String, String> producer = null;
    private Properties kafkaBrokerConfig = new Properties();
    private int zkMaxConnections;
    private int numOfBrokers;
    private ZkClient zkClient;
    private ZkUtils zkUtils;

    public KafkaUnit(int zkPort, int brokerPort) {
        this(zkPort, brokerPort, 16);
    }

    public KafkaUnit(int zkPort, int brokerPort, int zkMaxConnections) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
        this.zkMaxConnections = zkMaxConnections;
        withNumOfBrokers(1);
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString, int zkMaxConnections) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString), zkMaxConnections);
    }

    public KafkaUnit withNumOfBrokers(int numOfBrokers) {
        if (numOfBrokers < 1) {
            throw new IllegalArgumentException("numOfBrokers must be >= 1");
        }
        this.numOfBrokers = numOfBrokers;
        return this;
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

    private static int getEphemeralPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed finding an available port: "+e.getMessage(), e);
        }
    }

    public void startup() {
        startZookeeper();

        // ZKStringSerializer$.MODULE$ - this obscure object creation is what makes this client work
        // Without it , the topic is reported as created but is actually not usable and produces very hard
        // to understand behaviour
        zkClient = new ZkClient(getZookeeperString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        zkUtils = new ZkUtils(zkClient, new ZkConnection(getZookeeperString()), false);

        kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
        kafkaBrokerConfig.setProperty("host.name", "localhost");
        kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
        kafkaBrokerConfig.setProperty("delete.topic.enable", String.valueOf(true));
        kafkaBrokerConfig.setProperty("offsets.topic.replication.factor", String.valueOf(1));

        brokers = new ArrayList<>();

        // Maintain backward compatibility
        if (numOfBrokers == 1) {
            if (brokerPort == -1) {
                brokerPort = getEphemeralPort();
            }

            KafkaBroker kafkaBroker = startBroker(1, brokerPort);
            brokers.add(kafkaBroker);
        } else {
            for (int i = 1; i <= numOfBrokers; i++) {
                logger.info("Starting broker #"+i);
                brokers.add(startBroker(i, getEphemeralPort()));
            }
        }
        brokerString = "localhost:"+brokers.get(0).getPort();
    }

    private KafkaBroker startBroker(int brokerNum, int brokerPort) {
        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka-broker-"+brokerNum).toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka (Broker #"+brokerNum+")", e);
        }

        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    logger.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        }));

        Properties brokerConfig = new Properties();
        brokerConfig.putAll(kafkaBrokerConfig);
        brokerConfig.setProperty("broker.id", String.valueOf(brokerNum));
        brokerConfig.setProperty("port", Integer.toString(brokerPort));
        brokerConfig.setProperty("log.dir", logDir.getAbsolutePath());

        KafkaServerStartable broker = new KafkaServerStartable(new KafkaConfig(brokerConfig));
        broker.startup();

        return new KafkaBroker(brokerPort, broker);
    }

    private void startZookeeper() {
        zookeeper = new Zookeeper(zkPort, zkMaxConnections);
        zookeeper.startup();
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    /**
     * @return Broker port
     * @deprecated Use getBrokers()
     */
    @Deprecated
    public int getBrokerPort() {
        return brokerPort;
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, int numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperString;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + numPartitions;
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        try{
            // run
            logger.info("Executing: CreateTopic " + Arrays.toString(arguments));
            TopicCommand.createTopic(zkUtils, opts);
        } finally {
            zkUtils.close();
        }

    }

    /**
     *
     * @param partitionToBrokerReplicaAssignments Map from partition number to a set of brokers assignment per replica.
     * @param topicName The topic to reassign partition for
     */
    @SuppressWarnings("unchecked")
    public void reassignPartitions(String topicName, Map<Integer, Set<Integer>> partitionToBrokerReplicaAssignments) {
        logger.info("Executing: Reassigning partitions " + partitionToBrokerReplicaAssignments);

        Map<TopicAndPartition, Seq<Object>> partitionToBrokerReplicaAssignment = new HashMap<>();
        for (int partition : partitionToBrokerReplicaAssignments.keySet()) {
            Buffer<Object> brokers = JavaConversions.asScalaBuffer(new ArrayList(partitionToBrokerReplicaAssignments.get(partition)));
            partitionToBrokerReplicaAssignment.put(new TopicAndPartition(topicName, partition), brokers);
        }

        ReassignPartitionsCommand command = new ReassignPartitionsCommand(zkUtils, JavaConversions.mapAsScalaMap(partitionToBrokerReplicaAssignment));
        if (!command.reassignPartitions()) {
            throw new RuntimeException("Reassign failed");
        }
    }

    /**
     * @return All topic names
     */
    public List<String> listTopics(){
        String[] arguments = new String[3];
        arguments[0] = "--zookeeper";
        arguments[1] = zookeeperString;
        arguments[2] = "--list";
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        final List<String> topics = new ArrayList<>();
        try{
            // run
            logger.info("Executing: ListTopics " + Arrays.toString(arguments));

            PrintStream oldOut = Console.out();
            try{
                Console.setOut(new PrintStream(oldOut){
                    @Override
                    public void print(String s) {
                        super.print(s);
                        if(!s.endsWith("marked for deletion")){
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
    public void deleteAllTopics(){
        for (String topic: listTopics()){
            try{
                deleteTopic(topic);
            } catch (Throwable ignored){}
        }
    }

    /**
     * Delete a topic.
     * @param topicName The name of the topic to delete
     */
    public void deleteTopic(String topicName){
        String[] arguments = new String[5];
        arguments[0] = "--zookeeper";
        arguments[1] = zookeeperString;
        arguments[2] = "--delete";
        arguments[3] = "--topic";
        arguments[4] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());
        try{
            // run
            logger.info("Executing: DeleteTopic " + Arrays.toString(arguments));
            TopicCommand.deleteTopic(zkUtils, opts);
        } finally {
            zkUtils.close();
        }
    }

    public void shutdown() {
        for (int i = 0; i < brokers.size(); i++) {
            logger.info("Shutting down broker #{}", i+1);
            brokers.get(i).getKafkaServer().shutdown();
            brokers.get(i).getKafkaServer().awaitShutdown();
        }

        if (zkUtils != null) zkUtils.close();
        if (zkClient != null) zkClient.close();
        if (zookeeper != null) zookeeper.shutdown();
    }

    public List<KeyedMessage<String, String>> readKeyedMessages(final String topicName, final int expectedMessages) throws TimeoutException {
        return readMessages(topicName, expectedMessages, new MessageExtractor<KeyedMessage<String, String>>() {

            @Override
            public KeyedMessage<String, String> extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return new KeyedMessage<>(topicName, messageAndMetadata.key(), messageAndMetadata.message());
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

    public List<String> pollMessages(String topicName) throws TimeoutException {
        return readMessages(topicName, -1, new MessageExtractor<String>() {
            @Override
            public String extract(MessageAndMetadata<String, String> messageAndMetadata) {
                return messageAndMetadata.message();
            }
        });
    }

    private <T> List<T> readMessages(String topicName, final int expectedMessages, final MessageExtractor<T> messageExtractor) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", zookeeperString);
        consumerProperties.put("group.id", "10");
        consumerProperties.put("socket.timeout.ms", "500");
        consumerProperties.put("consumer.id", "test");
        consumerProperties.put("auto.offset.reset", "smallest");
        consumerProperties.put("consumer.timeout.ms", "500");
        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topicName, 1);
        Map<String, List<KafkaStream<String, String>>> events = javaConsumerConnector.createMessageStreams(topicMap, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> events1 = events.get(topicName);
        final KafkaStream<String, String> kafkaStreams = events1.get(0);


        Future<List<T>> submit = singleThread.submit(new Callable<List<T>>() {
            public List<T> call() throws Exception {
                List<T> messages = new ArrayList<>();
                try {
                    for (MessageAndMetadata<String, String> kafkaStream : kafkaStreams) {
                        T message = messageExtractor.extract(kafkaStream);
                        logger.debug("Received message: {}", kafkaStream.message());
                        messages.add(message);
                    }
                } catch (ConsumerTimeoutException e) {
                    // always gets throws reaching the end of the stream
                }
                if (expectedMessages >= 0 && messages.size() != expectedMessages) {
                    throw new ComparisonFailure("Incorrect number of messages returned", Integer.toString(expectedMessages),
                            Integer.toString(messages.size()));
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


    /**
     * @param message The 1st message to send
     * @param messages The rest of messages to send
     * @deprecated Use send(messages...)
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public final void sendMessages(KeyedMessage<String, String> message, KeyedMessage<String, String>... messages) {
        KeyedMessage<String, String>[] msgs = new KeyedMessage[messages.length + 1];
        System.arraycopy(messages, 0, msgs, 0, messages.length);
        msgs[messages.length] = message;

        send(msgs);
    }

    @SuppressWarnings("unchecked")
    public final void send(Collection<KeyedMessage<String, String>> messages) {
        send(messages.toArray(new KeyedMessage[messages.size()]));
    }

    @SafeVarargs
    public final void send(KeyedMessage<String, String>... messages) {
        if (producer == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerString);
            props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(10));
            producer = new KafkaProducer<>(props);
            logger.debug("Created kafka producer");
        }

        if (messages == null || messages.length == 0) return;

        List<Future<RecordMetadata>> futures = new ArrayList<>(messages.length);
        for (KeyedMessage<String, String> msg : messages) {
            logger.debug("Sent msg: "+msg);
            futures.add(producer.send(new ProducerRecord<>(msg.topic(), msg.key(), msg.message())));
        }
        logger.debug("Done sending messages. Now waiting for send to finish");
        for (Future<RecordMetadata> recordMetadataFuture : futures) {
            try {
                recordMetadataFuture.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        logger.info("Sent {} messages", messages.length);
    }

    /**
     * Set custom broker configuration.
     * @param configKey See available config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
     * @param configValue The values to set for the key
     */
    public void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    public List<KafkaBroker> getBrokers() {
        return Collections.unmodifiableList(brokers);
    }

    public String getZookeeperString() {
        return zookeeperString;
    }

    private interface MessageExtractor<T> {
        T extract(MessageAndMetadata<String, String> messageAndMetadata);
    }

    public static class KafkaBroker {
        private final int port;
        private final KafkaServerStartable kafkaServer;

        public KafkaBroker(int port, KafkaServerStartable kafkaServer) {
            this.port = port;
            this.kafkaServer = kafkaServer;
        }

        public int getPort() {
            return port;
        }

        public KafkaServerStartable getKafkaServer() {
            return kafkaServer;
        }
    }

}
