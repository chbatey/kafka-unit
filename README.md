# Kafka Unit Testing

![TravisCI](https://travis-ci.org/chbatey/kafka-unit.svg?branch=master)

Allows you to start and stop a Kafka broker + ZooKeeper instance for unit testing applications that communicate with Kafka.

It uses Kafka version ```0.8.2.2``` and ZooKeeper version ```3.4.6```

## Maven central

```xml
<dependency>
    <groupId>info.batey.kafka</groupId>
    <artifactId>kafka-unit</artifactId>
    <version>0.2</version>
</dependency>
```

## Starting manually

To start both a Kafka server and ZooKeeper instance, where the two numbers are the ZooKeeper port + the Kafka broker port.

```java
KafkaUnit kafkaUnitServer = new KafkaUnit(5000, 5001);
kafkaUnitServer.startup();
kafkaUnitServer.shutdown();
```

The alternative constructor allows providing connection strings rather than ports, which might be convenient if you want to use existing config without parsing it to extract port numbers:

```java
KafkaUnit kafkaUnitServer = new KafkaUnit("localhost:5000", "localhost:5001");
```

It's required that such a connection string consists of only one host:port pair - otherwise an exception will be thrown.

You can then write your own code to interact with Kafka or use the following methods:

```java
kafkaUnitServer.createTopic(testTopic);
KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");
kafkaUnitServer.sendMessages(keyedMessage);
```

And to read messages:

```java
List<String> messages = kafkaUnitServer.readMessages(testTopic, 1);
```

Only String messages are supported at the moment.

## Using the JUnit Rule

If you don't want to start/stop the server manually you can use the JUnit rule. E.g

```java
public class KafkaUnitIntegrationTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(6000, 6001);

    @Test
    public void junitRuleShouldHaveStartedKafka() throws Exception {
        String testTopic = "TestTopic";
        kafkaUnitRule.getKafkaUnit().createTopic(testTopic);
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(testTopic, "key", "value");

        kafkaUnitRule.getKafkaUnit().sendMessages(keyedMessage);
        List<String> messages = kafkaUnitRule.getKafkaUnit().readMessages(testTopic, 1);

        assertEquals(Arrays.asList("value"), messages);
    }
}
```

This will start/stop the broker every test so that one test can't interfere with the next.

## License

```
Copyright 2013 Christopher Batey

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

