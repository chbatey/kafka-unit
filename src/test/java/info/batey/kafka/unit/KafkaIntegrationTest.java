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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaIntegrationTest {

    private KafkaUnit kafkaUnitServer;

    @Before
    public void setUp() throws Exception {
        kafkaUnitServer = new KafkaUnit(5000, 5001);
        kafkaUnitServer.setKafkaBrokerConfig("log.segment.bytes", "1024");
        kafkaUnitServer.startup();
    }

    @After
    public void shutdown() throws Exception {
        Field f = kafkaUnitServer.getClass().getDeclaredField("broker");
        f.setAccessible(true);
        KafkaServerStartable broker = (KafkaServerStartable) f.get(kafkaUnitServer);
        assertEquals(1024, (int) broker.serverConfig().logSegmentBytes());
        kafkaUnitServer.deleteAllTopics();
        kafkaUnitServer.shutdown();
    }

    @Test
    public void kafkaServerIsAvailable() throws Exception {
        assertKafkaServerIsAvailable(kafkaUnitServer);
    }

    @Test
    public void canDeleteTopic() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic,
            "key",
            "value");

        //when
        kafkaUnitServer.sendMessages(keyedMessage);
        assertEquals(kafkaUnitServer.readAllMessages(testTopic), Collections.singletonList("value"));
        kafkaUnitServer.deleteTopic(testTopic);
        assertEquals(kafkaUnitServer.readAllMessages(testTopic), Collections.<String>emptyList());
    }

    @Test
    public void canListTopics() throws Exception {
        String testTopic1 = "TestTopic1";
        kafkaUnitServer.createTopic(testTopic1);
        String testTopic2 = "TestTopic2";
        kafkaUnitServer.createTopic(testTopic2);

        List<String> topics = kafkaUnitServer.listTopics();
        assertEquals(2, topics.size());
        assertTrue(topics.contains(testTopic1));
        assertTrue(topics.contains(testTopic2));
    }

    @Test
    public void canDeleteAllTopics() {
        String testTopic1 = "TestTopic1";
        kafkaUnitServer.createTopic(testTopic1);
        String testTopic2 = "TestTopic2";
        kafkaUnitServer.createTopic(testTopic2);

        List<String> topics = kafkaUnitServer.listTopics();
        assertTrue(topics.contains(testTopic1));
        assertTrue(topics.contains(testTopic2));

        kafkaUnitServer.deleteAllTopics();
        topics = kafkaUnitServer.listTopics();
        assertFalse(topics.contains(testTopic1));
        assertFalse(topics.contains(testTopic2));
    }

    @Test
    public void startKafkaServerWithoutParamsAndSendMessage() throws Exception {
        KafkaUnit noParamServer = new KafkaUnit();
        noParamServer.startup();
        assertKafkaServerIsAvailable(noParamServer);
        assertTrue("Kafka port needs to be non-negative", noParamServer.getBrokerPort() > 0);
        assertTrue("Zookeeper port needs to be non-negative", noParamServer.getZkPort() > 0);
    }

    @Test
    public void canUseKafkaConnectToProduce() throws Exception {
        final String topic = "KafkaConnectTestTopic";
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect());
        Producer<Long, String> producer = new KafkaProducer<>(props);
        ProducerRecord<Long, String> record = new ProducerRecord<>(topic, 1L, "test");
        producer.send(record);      // would be good to have KafkaUnit.sendMessages() support the new producer
        assertEquals("test", kafkaUnitServer.readMessages(topic, 1).get(0));
    }

    @Test
    public void canReadRecords() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic,
            "key",
            "value");

        //when
        kafkaUnitServer.sendMessages(keyedMessage);

        ConsumerRecord<String, String> receivedMessage = kafkaUnitServer.readRecords(testTopic, 1).get(0);

        assertEquals("Received message value is incorrect", "value", receivedMessage.value());
        assertEquals("Received message key is incorrect", "key", receivedMessage.key());
        assertEquals("Received message topic is incorrect", testTopic, receivedMessage.topic());
    }

    @Test(timeout = 30000)
    public void closeConnectionBetweenTopicCreations() throws Exception{
        String topicPrefix = "TestTopic";
        for(int i = 0; i < 17; i++){
            kafkaUnitServer.createTopic(topicPrefix + i);
        }
    }

    private void assertKafkaServerIsAvailable(KafkaUnit server) {
        //given
        String testTopic = "TestTopic";
        server.createTopic(testTopic);
        ProducerRecord<String, String> message = new ProducerRecord<>(testTopic,
            "key",
            "value");
        //when
        server.sendMessages(message);
        List<String> messages = server.readMessages(testTopic, 1);
        //then
        assertEquals(Collections.singletonList("value"), messages);
    }
}