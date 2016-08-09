package info.batey.kafka.unit;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;


public abstract class AbstractKafkaUnit {

    protected static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);
    protected KafkaServerStartable broker;
    protected Zookeeper zookeeper;
    protected String zookeeperUri;
    protected String brokerString;
    protected int zkPort;
    protected int brokerPort;
    protected String certStorePath = "";
    protected int timeout_3_Seconds = 3000;
    protected Properties kafkaBrokerConfig = new Properties();

    abstract Properties getProducerConfig();

    abstract void setBrokerConfig();

    public void startup() {
        setBrokerConfig();
        zookeeper = new Zookeeper(zkPort);
        zookeeper.startup();
        final KafkaConfig serverConfig = new KafkaConfig(kafkaBrokerConfig);
        broker = new KafkaServerStartable(serverConfig);
        broker.startup();
    }

    @SafeVarargs
    public final void sendMessages(ProducerRecord<String, String> message, ProducerRecord<String, String>... messages) {

        try (Producer<String, String> producer = createProducer();) {
            producer.send(message);
            for (ProducerRecord<String, String> msg : messages) {
                producer.send(msg);
            }
        }
    }

    private Producer<String, String> createProducer() {
        return new KafkaProducer<>(getProducerConfig());
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperUri;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled()
        );

        // run
        LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.createTopic(zkUtils, opts);
    }

    /* Set custom broker configuration.
     * See avaliable config keys in the kafka documentation: http://kafka.apache.org/documentation
     * .html#brokerconfigs
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void shutdown() {

        if (broker != null) {
            broker.shutdown();
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }

    protected static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    protected File getLogDirectory() {
        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }
        logDir.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.deleteDirectory(logDir);
                } catch (IOException e) {
                    LOGGER.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
                }
            }
        }));
        return logDir;
    }
}
