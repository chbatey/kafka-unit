package info.batey.kafka.unit;

import kafka.admin.CreateTopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

public class Kafka {

    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka.class);

    private KafkaServerStartable broker;
    private Zookeeper zookeeper;
    private final String zookeeperString;
    private int zkPort;
    private int brokerPort;

    public Kafka(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperString = "localhost:" + zkPort;
    }

    public void startup() {

        zookeeper = new Zookeeper(zkPort);
        zookeeper.startup();

        File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", zookeeperString);
        properties.setProperty("broker.id", "1");
        properties.setProperty("host.name", "localhost");
        properties.setProperty("port", Integer.toString(brokerPort));
        properties.setProperty("log.dir", logDir.getAbsolutePath());
        properties.setProperty("log.flush.interval.messages", String.valueOf(1));

        broker = new KafkaServerStartable(new KafkaConfig(properties));
        broker.startup();
    }

    public void createTopic(String topicName) {
        String [] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = zookeeperString;
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "1";
        arguments[6] = "--topic";
        arguments[7] = topicName;
        LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
        CreateTopicCommand.main(arguments);
    }


    public void shutdown() {
        if (broker != null) broker.shutdown();
        if (zookeeper != null) zookeeper.shutdown();
    }

    public List<String> readMessages(String topicName, int expectedMessages) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", zookeeperString);
        consumerProperties.put("group.id", "10");
        consumerProperties.put("socket.timeout.ms", "500");
        consumerProperties.put("consumer.id", "test");
        consumerProperties.put("auto.offset.reset", "smallest");
        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topicName, 1);
        Map<String, List<KafkaStream<String, String>>> events = javaConsumerConnector.createMessageStreams(topicMap, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> events1 = events.get(topicName);
        KafkaStream<String, String> kafkaStreams = events1.get(0);

        Future<List<String>> submit = singleThread.submit(() -> {
            List<String> messages = new ArrayList<>();
            ConsumerIterator<String, String> iterator = kafkaStreams.iterator();
            while (messages.size() != expectedMessages && iterator.hasNext()) {
                String message = iterator.next().message();
                LOGGER.info("Received message: {}", message);
                messages.add(message);
            }
            return messages;
        });

        try {
            return submit.get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            singleThread.shutdown();
            javaConsumerConnector.shutdown();
        }
    }
}

