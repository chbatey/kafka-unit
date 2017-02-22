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

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import kafka.producer.KeyedMessage;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
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
        assertEquals(1024, (int)broker.serverConfig().logSegmentBytes());

        kafkaUnitServer.shutdown();
    }

    @Test
    public void kafkaServerIsAvailable() throws Exception {
        assertKafkaServerIsAvailable(kafkaUnitServer);
    }

    @Test(expected = ComparisonFailure.class)
    public void shouldThrowComparisonFailureIfMoreMessagesRequestedThanSent() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        //when
        kafkaUnitServer.sendMessages(keyedMessage);

        try {
            kafkaUnitServer.readMessages(testTopic, 2);
            fail("Expected ComparisonFailure to be thrown");
        } catch (ComparisonFailure e) {
            assertEquals("Wrong value for 'expected'", "2", e.getExpected());
            assertEquals("Wrong value for 'actual'", "1", e.getActual());
            assertEquals("Wrong error message", "Incorrect number of messages returned", e.getMessage());
        }
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
        final String topic = "KafkakConnectTestTopic";
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
    public void  canReadKeyedMessages() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        //when
        kafkaUnitServer.sendMessages(keyedMessage);

        KeyedMessage<String, String> receivedMessage = kafkaUnitServer.readKeyedMessages(testTopic, 1).get(0);

        assertEquals("Received message value is incorrect", "value", receivedMessage.message());
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

    private void assertKafkaServerIsAvailable(KafkaUnit server) throws TimeoutException {
        //given
        String testTopic = "TestTopic";
        server.createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        //when
        server.sendMessages(keyedMessage);
        List<String> messages = server.readMessages(testTopic, 1);

        //then
        assertEquals(Arrays.asList("value"), messages);
    }
}