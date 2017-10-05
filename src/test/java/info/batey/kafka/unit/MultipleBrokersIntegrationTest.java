package info.batey.kafka.unit;

import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class MultipleBrokersIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(MultipleBrokersIntegrationTest.class);
    private KafkaUnit kafkaUnit;

    @Before
    public void before() {
        kafkaUnit = new KafkaUnit().withNumOfBrokers(2);
        kafkaUnit.startup();
    }

    @After
    public void after() {
        kafkaUnit.shutdown();
    }

    @Test
    public void sendMessage() throws TimeoutException {
        String topicName = "bla";
        kafkaUnit.createTopic(topicName);
        kafkaUnit.send(new KeyedMessage<String, String>(topicName, "hello"));
        kafkaUnit.readKeyedMessages(topicName, 1);
    }

    @Test
    public void movePartitions() throws TimeoutException, InterruptedException {
        String topic = "bla";
        int partitions = 50;
        kafkaUnit.createTopic(topic, partitions);

        createMsgsInTopic(topic, 1000);
        kafkaUnit.readMessages(topic, 1000);

        mapAllTopicPartitionsToSpecificBroker(topic, 2);
        waitUntilAllTopicPartitionsToBeAssignedToBroker(topic, 2);

        int msgCount = 100;
        createMsgsInTopic(topic, msgCount);
        kafkaUnit.readMessages(topic, msgCount);
    }

    private void mapAllTopicPartitionsToSpecificBroker(String topic, int brokerNumber) {
        KafkaConsumer<String, String> consumer = createConsumer("testMeMap");
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        Set<Integer> partitionNotMappedToSpecifiedBroker = new HashSet<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            assertThat(partitionInfo.replicas().length).isEqualTo(1);
            if (partitionInfo.replicas()[0].id() != brokerNumber) {
                partitionNotMappedToSpecifiedBroker.add(partitionInfo.partition());
            }
        }

        // We have only a single replica - let's assign it to brokerNumber
        Set<Integer> brokers = Collections.singleton(brokerNumber);

        Map<Integer, Set<Integer>> partitionToBroker = new HashMap<>();
        for (int partitionNum : partitionNotMappedToSpecifiedBroker) {
            partitionToBroker.put(partitionNum, brokers);
        }

        logger.info("Reassigning partitions");
        kafkaUnit.reassignPartitions(topic, partitionToBroker);
    }

    private void waitUntilAllTopicPartitionsToBeAssignedToBroker(final String topic, final int brokerNum) {
        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                KafkaConsumer<String, String> consumer = createConsumer("testMeMap");
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    if (partitionInfo.leader().id() != brokerNum) {
                        logger.debug("Partition {} leader is mapped to broker num {}", partitionInfo.partition(),
                                partitionInfo.leader().id());
                        Thread.sleep(1000);
                        return false;
                    }
                    for (Node node : partitionInfo.replicas()) {
                        if (node.id() != brokerNum) {
                            logger.debug("Partition {} replica is mapped to broker num {}", partitionInfo.partition(),
                                    node.id());
                            Thread.sleep(1000);
                            return false;
                        }
                    }
                }
                return true;
            }
        });
    }

    private void createMsgsInTopic(String topic, int msgCount) throws TimeoutException {
        List<KeyedMessage<String, String>> msgs = new ArrayList<>();
        for (int i = 0; i < msgCount; i++) {
            msgs.add(new KeyedMessage<String, String>(topic, "bla bla bla"));

        }
        kafkaUnit.send(msgs);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnit.getKafkaConnect());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(configs);
    }
}
