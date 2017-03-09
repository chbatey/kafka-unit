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

import org.junit.rules.ExternalResource;

import java.io.IOException;

public class KafkaUnitRule extends ExternalResource {

    private final KafkaUnit kafkaUnit;

    public KafkaUnitRule() {
        try {
            this.kafkaUnit = new KafkaUnit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KafkaUnitRule(int zkPort, int kafkaPort) {
        this.kafkaUnit = new KafkaUnit(zkPort, kafkaPort);
    }

    public KafkaUnitRule(String zkConnectionString, String kafkaConnectionString) {
        this.kafkaUnit = new KafkaUnit(zkConnectionString, kafkaConnectionString);
    }
    
    public KafkaUnitRule(int zkPort, int kafkaPort, int zkMaxConnections) {
        this.kafkaUnit = new KafkaUnit(zkPort, kafkaPort, zkMaxConnections);
    }

    public KafkaUnitRule(String zkConnectionString, String kafkaConnectionString, int zkMaxConnections) {
        this.kafkaUnit = new KafkaUnit(zkConnectionString, kafkaConnectionString, zkMaxConnections);
    }

    @Override
    protected void before() throws Throwable {
        kafkaUnit.startup();
    }

    @Override
    protected void after() {
        kafkaUnit.shutdown();
    }

    public int getZkPort() {
        return kafkaUnit.getZkPort();
    }

    public int getKafkaPort() {
        return kafkaUnit.getBrokerPort();
    }

    public KafkaUnit getKafkaUnit() {
        return kafkaUnit;
    }
}
