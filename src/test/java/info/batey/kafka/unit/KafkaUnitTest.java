package info.batey.kafka.unit;


import org.junit.Test;

public class KafkaUnitTest {

    @Test
    public void successfullyConstructKafkaUnitFromConnectionStrings() {
        new KafkaUnit("localhost:2181", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromZooKeeperConnectionStringWithNonNumericPort() {
        new KafkaUnit("localhost:abcd", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromKafkaConnectionStringWithNonNumericPort() {
        new KafkaUnit("localhost:2181", "localhost:abcd");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromZooKeeperConnectionStringWithMultipleHosts() {
        new KafkaUnit("localhost:2181,localhost:2182", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromKafkaConnectionStringWithMultipleHosts() {
        new KafkaUnit("localhost:2181", "localhost:9092,localhost:9093");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromInvalidZooKeeperConnectionString() {
        new KafkaUnit("localhost-2181", "localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void failToConstructKafkaUnitFromInvalidKafkaConnectionString() {
        new KafkaUnit("localhost:2181", "localhost-9092");
    }

    @Test
    public void successfullyConstructKafkaUnitWithoutAnyParams() throws Exception {
        new KafkaUnit();
    }
}
