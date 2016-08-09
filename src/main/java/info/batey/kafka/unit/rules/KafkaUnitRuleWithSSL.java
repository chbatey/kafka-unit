package info.batey.kafka.unit.rules;

import info.batey.kafka.unit.KafkaUnitWithSSL;
import org.junit.rules.ExternalResource;

public class KafkaUnitRuleWithSSL extends ExternalResource {

    private final KafkaUnitWithSSL kafkaUnit;

    public KafkaUnitRuleWithSSL(int zkPort, int kafkaPort) {
        this.kafkaUnit = new KafkaUnitWithSSL(zkPort, kafkaPort);
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

    public KafkaUnitWithSSL getKafkaUnit() {
        return kafkaUnit;
    }
}
