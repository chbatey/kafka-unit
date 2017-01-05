# Kafka Unit Testing

![TravisCI](https://travis-ci.org/chbatey/kafka-unit.svg?branch=master)

Allows you to start and stop a Kafka broker + ZooKeeper instance for unit testing applications that communicate with Kafka.

## Versions
| kafka-unit | Kafka broker            | Zookeeper |
|------------|-------------------------|-----------|
| 0.7        | kafka_2.11:0.10.1.1     | 3.4.6     |
| 0.6        | kafka_2.11:0.10.0.0     | 3.4.6     |
| 0.5        | kafka_2.11:0.9.0.1      | 3.4.6     |
| 0.4        | kafka_2.11:0.9.0.1      | 3.4.6     |
| 0.3        | kafka_2.11:0.8.2.2      | 3.4.6     |
| 0.2        | kafka_2.11:0.8.2.1      | 3.4.6     |

## Maven central

```xml
<dependency>
    <groupId>info.batey.kafka</groupId>
    <artifactId>kafka-unit</artifactId>
    <version>0.7</version>
</dependency>
```

## Starting manually

To start both a Kafka server and ZooKeeper instance on random ports use following code:

```java
KafkaUnit kafkaUnitServer = new KafkaUnit();
kafkaUnitServer.startup();
kafkaUnitServer.shutdown();
```

ZooKeeper and Kafka broker ports can be specified explicitly using second constructor, which takes two `int`s:

```java
KafkaUnit kafkaUnitServer = new KafkaUnit(5000, 5001);
```

The alternative constructor allows providing connection strings rather than ports, which might be convenient if you want to use existing config without parsing it to extract port numbers:

```java
KafkaUnit kafkaUnitServer = new KafkaUnit("localhost:5000", "localhost:5001");
```

Currently only `localhost` is supported and it's required that the connection string consists of only one `localhost:[port]` pair.

You can then write your own code to interact with Kafka or use the following methods:

```java
kafkaUnitServer.createTopic(testTopic);
ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic, "key", "value");
kafkaUnitServer.sendRecord(producerRecord);
```

And to read messages:

```java
List<String> messages = kafkaUnitServer.readMessages(testTopic, 1);
```

Only `String` messages are supported at the moment.

Alternatively, you can use `getKafkaConnect()` to manually configure producer and consumer clients like:

```java
Properties props = new Properties();
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getCanonicalName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUnitServer.getKafkaConnect());

Producer<Long, String> producer = new KafkaProducer<>(props);
```

## Using the JUnit Rule

If you don't want to start/stop the server manually, you can use the JUnit rule, e.g.

```java
public class KafkaUnitIntegrationTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    @Test
    public void junitRuleShouldHaveStartedKafka() throws Exception {
        String testTopic = "TestTopic";
        kafkaUnitRule.getKafkaUnit().createTopic(testTopic);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic, "key", "value");

        kafkaUnitRule.getKafkaUnit().sendRecords(producerRecord);
        List<String> messages = kafkaUnitRule.getKafkaUnit().readMessages(testTopic, 1);

        assertEquals(Arrays.asList("value"), messages);
    }
}
```

This will start/stop the broker every test, so that particular test can't interfere with the next. 
Contrary to `KafkaUnit()` constructor, it does not throw checked `IOException` when socket initialization fails, but wraps it in runtime exception and thus is suitable for use as `@Rule` field in tests.

If you want to start server on specific ports, use `KafkaUnitRule(int, int)` or `KafkaUnitRule(String, String)` constructor, which accepts ZooKeeper and Kafka broker ports or connection strings respectively (just like corresponding `KafkaUnit` constructors), e.g.:

```java
    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(5000, 5001);
```

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

