# Kafka Unit Testing

![TravisCI](https://travis-ci.org/chbatey/kafka-unit.svg?branch=master)

Allows you to start and stop a Kafka broker + ZooKeeper instance for unit testing applications that communicate with Kafka.

## Versions
| kafka-unit | Kafka broker | Zookeeper |
|------------|--------------|-----------|
| 0.1.4      | 0.9.0.1      | 3.4.6     |
| 0.1.3      | 0.8.2.1      | 3.4.6     |

## Maven central

```xml
<dependency>
    <groupId>info.batey.kafka</groupId>
    <artifactId>kafka-unit</artifactId>
    <version>0.1.4</version>
</dependency>
```

## Starting manually

To start both a Kafka server and ZooKeeper instance, where the two numbers are the ZooKeeper port + the Kafka broker port.

```java
KafkaUnit kafkaUnitServer = new KafkaUnit(5000, 5001);
kafkaUnitServer.startup();
kafkaUnitServer.shutdown();
```

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

