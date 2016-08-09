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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_HOST_NAME;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_ID;
import static info.batey.kafka.unit.KafkaUnitConfig.BROKER_PORT;
import static info.batey.kafka.unit.KafkaUnitConfig.CONSUMER_ID;
import static info.batey.kafka.unit.KafkaUnitConfig.CONSUMER_SOCKET_TIMEOUT_MS;
import static info.batey.kafka.unit.KafkaUnitConfig.CONSUMER_TIMEOUT_MS;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_CONNECT;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_LOG_DIRECTORY;
import static info.batey.kafka.unit.KafkaUnitConfig.ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES;
import static java.lang.String.valueOf;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaUnit extends AbstractKafkaUnit {

    public KafkaUnit(int zkPort, int brokerPort) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperUri = "localhost:" + zkPort;
        this.brokerString = "localhost:" + brokerPort;
    }

    public KafkaUnit(String zkConnectionString, String kafkaConnectionString) {
        this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
    }

    @Override void setBrokerConfig() {
        final File logDir = getLogDirectory();
        kafkaBrokerConfig.setProperty(ZOOKEEPER_CONNECT, zookeeperUri);
        kafkaBrokerConfig.setProperty(BROKER_ID, "1");
        kafkaBrokerConfig.setProperty(BROKER_HOST_NAME, "localhost");
        kafkaBrokerConfig.setProperty(BROKER_PORT, Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_DIRECTORY, logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES, valueOf(1));
    }

    @Override Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
        return props;
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
        consumerProperties.put(ZOOKEEPER_CONNECT, zookeeperUri);
        consumerProperties.put(CONSUMER_SOCKET_TIMEOUT_MS, "500");
        consumerProperties.put(CONSUMER_ID, "test");
        consumerProperties.put(CONSUMER_TIMEOUT_MS, "500");
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

    private interface MessageExtractor<T> {
        T extract(MessageAndMetadata<String, String> messageAndMetadata);
    }
}
