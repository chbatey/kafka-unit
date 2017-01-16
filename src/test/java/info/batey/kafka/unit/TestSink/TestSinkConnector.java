package info.batey.kafka.unit.TestSink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    public String version() {
        return "1.0";
    }

    public void start(Map<String, String> props) {
        configProperties = props;
    }

    public Class<? extends Task> taskClass() {
        return TestSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>(maxTasks);
        ConfigDef def = config();

        while (configs.size() < maxTasks) {
            HashMap<String, String> config = new HashMap<>();

            for (String key : def.names()) {
                String value = configProperties.get(key);
                config.put(key, value);
            }

            configs.add(config);
        }

        return configs;
    }

    public void stop() {
    }

    public ConfigDef config() {
        return new ConfigDef();
    }
}