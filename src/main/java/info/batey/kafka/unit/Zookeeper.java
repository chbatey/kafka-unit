/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.batey.kafka.unit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Zookeeper {
    private int port;
    private int maxConnections;

    private ServerCnxnFactory factory;

    public Zookeeper(int port) {
        this.port = port;
        this.maxConnections = 16;
    }
    
    public Zookeeper(int port, int maxConnections) {
        this.port = port;
        this.maxConnections = maxConnections;
    }

    public void startup() {

        final File snapshotDir;
        final File logDir;
        try {
            snapshotDir = java.nio.file.Files.createTempDirectory("zookeeper-snapshot").toFile();
            logDir = java.nio.file.Files.createTempDirectory("zookeeper-logs").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    FileUtils.deleteDirectory(snapshotDir);
                    FileUtils.deleteDirectory(logDir);
                }
                catch(IOException e) {
                    // We tried!
                }
            }

        });

        try {
            int tickTime = 500;
            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            this.factory = NIOServerCnxnFactory.createFactory();
            this.factory.configure(new InetSocketAddress("localhost", port), maxConnections);
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