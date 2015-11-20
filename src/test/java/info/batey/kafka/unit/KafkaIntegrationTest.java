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

import kafka.producer.KeyedMessage;
import kafka.server.KafkaServerStartable;

import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        assertEquals(1024, broker.serverConfig().logSegmentBytes());

        kafkaUnitServer.shutdown();
    }

    @Test
    public void kafkaServerIsAvailable() throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnitServer.createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        //when
        kafkaUnitServer.sendMessages(keyedMessage);
        List<String> messages = kafkaUnitServer.readMessages(testTopic, 1);

        //then
        assertEquals(Arrays.asList("value"), messages);
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
}