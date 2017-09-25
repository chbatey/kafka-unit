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

import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class KafkaUnitRuleIntegrationTestBase
{
    protected void assertKafkaStartsAndSendsMessage(final KafkaUnit kafkaUnit) throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnit.createTopic(testTopic);
        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic,
            "key",
            "value");
        //when
        kafkaUnit.sendMessages(keyedMessage);
        List<String> messages = kafkaUnit.readMessages(testTopic, 1);
        //then
        assertEquals(Collections.singletonList("value"), messages);
    }
}
