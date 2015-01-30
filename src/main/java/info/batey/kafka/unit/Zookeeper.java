package info.batey.kafka.unit;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Zookeeper {
    private int port;

    private ServerCnxnFactory factory;

    public Zookeeper(int port) {
        this.port = port;
    }

    public void startup() {

        File snapshotDir;
        File logDir;
        try {
            snapshotDir = java.nio.file.Files.createTempDirectory("zookeeper-snapshot").toFile();
            logDir = java.nio.file.Files.createTempDirectory("zookeeper-logs").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }

        snapshotDir.deleteOnExit();
        logDir.deleteOnExit();

        try {
            int tickTime = 500;
            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            this.factory = NIOServerCnxnFactory.createFactory();
            this.factory.configure(new InetSocketAddress("localhost", port), 16);
            factory.startup(zkServer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start ZooKeeper", e);
        }
    }

    public void shutdown() {
        factory.shutdown();
    }
}