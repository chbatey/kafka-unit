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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KafkaUnitIntegrationTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(6000, 6001);

    @Rule
    public KafkaUnitRule kafkaUnitRuleWithConnectionStrings = new KafkaUnitRule("localhost:5000", "localhost:5001");

    @Rule
    public KafkaUnitRule kafkaUnitRuleWithEphemeralPorts = new KafkaUnitRule();

    @Test
    public void junitRuleShouldHaveStartedKafka() throws Exception {
        assertKafkaStartsAndSendsMessage(kafkaUnitRule.getKafkaUnit());
    }

    @Test
    public void junitRuleShouldHaveStartedKafkaWithConnectionStrings() throws Exception {
        assertKafkaStartsAndSendsMessage(kafkaUnitRuleWithConnectionStrings.getKafkaUnit());
    }

    @Test
    public void junitRuleShouldHaveStartedKafkaWithEphemeralPorts() throws Exception {
        assertKafkaStartsAndSendsMessage(kafkaUnitRuleWithEphemeralPorts.getKafkaUnit());
    }

    private void assertKafkaStartsAndSendsMessage(final KafkaUnit kafkaUnit) throws Exception {
        //given
        String testTopic = "TestTopic";
        kafkaUnit.createTopic(testTopic);
        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "key", "value");

        //when
        kafkaUnitRule.getKafkaUnit().sendMessages(keyedMessage);
        List<String> messages = kafkaUnitRule.getKafkaUnit().readMessages(testTopic, 1);

        //then
        assertEquals(Collections.singletonList("value"), messages);
    }
}
