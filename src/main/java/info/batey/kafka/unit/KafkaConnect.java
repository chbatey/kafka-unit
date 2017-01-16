package info.batey.kafka.unit;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;


public class KafkaConnect {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);

    private Connect connect;

    public KafkaConnect(Map<String, String> workerProps, Map<String, String> connectorProps) {

        Time time = new SystemTime();
        ConnectorFactory connectorFactory = new ConnectorFactory();
        StandaloneConfig config = new StandaloneConfig(workerProps);

        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        Worker worker = new Worker(workerId, time, connectorFactory, config, new FileOffsetBackingStore());

        Herder herder = new StandaloneHerder(worker);
        connect = new Connect(herder, rest);

        try {
            connect.start();

            FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
                @Override
                public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                    if (error != null)
                        LOGGER.error("Failed to create job");
                    else
                        LOGGER.info("Created connector {}", info.result().name());
                }
            });
            herder.putConnectorConfig(
                    connectorProps.get(ConnectorConfig.NAME_CONFIG),
                    connectorProps, false, cb);
            cb.get();

        } catch (Throwable t) {
            LOGGER.error("Stopping after connector error", t);
            connect.stop();
        }
    }

    public void shutdown() {
        connect.stop();
    }
}
