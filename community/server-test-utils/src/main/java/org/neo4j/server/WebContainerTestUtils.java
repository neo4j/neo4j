/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class WebContainerTestUtils {
    private WebContainerTestUtils() {}

    public static Path createTempDir() throws IOException {
        return Files.createTempDirectory("neo4j-test");
    }

    public static Path getRelativePath(Path folder, Setting<Path> setting) {
        return folder.resolve(setting.defaultValue());
    }

    public static Map<String, String> getDefaultRelativeProperties(Path folder) {
        Map<String, String> settings = new HashMap<>();
        addDefaultRelativeProperties(settings, folder);
        return settings;
    }

    public static void addDefaultRelativeProperties(Map<String, String> properties, Path temporaryFolder) {
        addRelativeProperty(temporaryFolder, properties, GraphDatabaseSettings.data_directory);
        addRelativeProperty(temporaryFolder, properties, GraphDatabaseSettings.logs_directory);
        properties.put(GraphDatabaseSettings.pagecache_memory.name(), "8m");
    }

    private static void addRelativeProperty(
            Path temporaryFolder, Map<String, String> properties, Setting<Path> setting) {
        properties.put(setting.name(), getRelativePath(temporaryFolder, setting).toString());
    }

    public static void writeConfigToFile(Map<String, String> properties, Path file) {
        Properties props = loadProperties(file);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        storeProperties(file, props);
    }

    public static String asOneLine(Map<String, String> properties) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            builder.append(builder.length() > 0 ? "," : "");
            builder.append(property.getKey()).append('=').append(property.getValue());
        }
        return builder.toString();
    }

    private static void storeProperties(Path file, Properties properties) {
        try (OutputStream out = Files.newOutputStream(file)) {
            properties.store(out, "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties loadProperties(Path file) {
        Properties properties = new Properties();
        if (Files.exists(file)) {
            try (InputStream in = Files.newInputStream(file)) {
                properties.load(in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return properties;
    }

    public static Path createTempConfigFile(Path parentDir) {
        return parentDir.resolve("test-" + new Random().nextInt() + ".properties");
    }

    public interface BlockWithCSVFileURL {
        void execute(String url) throws Exception;
    }

    public static void withCSVFile(int rowCount, BlockWithCSVFileURL block) throws Exception {
        Path file = Files.createTempFile("file", ".csv");
        try {
            try (PrintWriter writer = new PrintWriter(Files.newOutputStream(file))) {
                for (int i = 0; i < rowCount; ++i) {
                    writer.println("1,2,3");
                }
            }

            String url = file.toUri().toURL().toString().replace("\\", "\\\\");
            block.execute(url);
        } finally {
            Files.delete(file);
        }
    }

    public static void verifyConnector(GraphDatabaseService db, ConnectorType connectorType, boolean enabled) {
        HostnamePort address = connectorAddress(db, connectorType);
        if (enabled) {
            assertThat(address).isNotNull();
            assertThat(canConnectToSocket(address.getHost(), address.getPort())).isTrue();
        } else {
            assertThat(address).isNull();
        }
    }

    public static HostnamePort connectorAddress(GraphDatabaseService db, ConnectorType connectorType) {
        ConnectorPortRegister portRegister =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(ConnectorPortRegister.class);
        return portRegister.getLocalAddress(connectorType);
    }

    private static boolean canConnectToSocket(String host, int port) {
        try {
            new Socket(host, port).close();
            return true;
        } catch (Throwable ignore) {
            return false;
        }
    }
}
