package info.batey.kafka.unit;

import org.junit.rules.ExternalResource;

public class KafkaUnitRule extends ExternalResource {

    private final int zkPort;
    private final int kafkaPort;
    private final KafkaUnit kafkaUnit;

    public KafkaUnitRule(int zkPort, int kafkaPort) {
        this.zkPort = zkPort;
        this.kafkaPort = kafkaPort;
        this.kafkaUnit = new KafkaUnit(zkPort, kafkaPort);
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
        return zkPort;
    }

    public int getKafkaPort() {
        return kafkaPort;
    }

    public KafkaUnit getKafkaUnit() {
        return kafkaUnit;
    }
}
