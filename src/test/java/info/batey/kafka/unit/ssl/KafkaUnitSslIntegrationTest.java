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
package info.batey.kafka.unit.ssl;

import info.batey.kafka.unit.KafkaUnitRule;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

public class KafkaUnitSslIntegrationTest {

    private static final int ZOOKEEPER_PORT = 6000;
    private static final int BROKER_PORT = 6001;
    private static final String KEYSTORE_PATH = "/Users/lakitu/repos/kafka-unit/src/main/resources/";

    @Rule
    public KafkaUnitRule kafkaUnitSslRule = new KafkaUnitRule(ZOOKEEPER_PORT, BROKER_PORT, true);

    @Test
    public void junitRuleShouldHaveStartedKafkaWithSsl() throws Exception {
        //given
        Random random = new Random();
        String testTopic = "test-topic";
        kafkaUnitSslRule.getKafkaUnit().createTopic(testTopic);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic,
            "key",
            "test value :" + random.nextLong()
        );

        //when
        kafkaUnitSslRule.getKafkaUnit().sendMessages(producerRecord);

        // then
       kafkaUnitSslRule.getKafkaUnit().readMessagesOverSSL(testTopic, 1);

    }

}
