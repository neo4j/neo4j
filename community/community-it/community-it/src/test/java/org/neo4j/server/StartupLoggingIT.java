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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.io.fs.FileSystemUtils.readLines;
import static org.neo4j.server.AbstractNeoWebServer.NEO4J_IS_STARTING_MESSAGE;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class StartupLoggingIT extends ExclusiveWebContainerTestBase {
    @Test
    void shouldLogHelpfulStartupMessages() throws IOException {
        CommunityBootstrapper bootstrapper = new CommunityBootstrapper();
        Map<String, String> propertyPairs = getPropertyPairs();

        bootstrapper.start(testDirectory.homePath(), Path.of("nonexistent-file.conf"), propertyPairs, false, false);
        var resolver = getDependencyResolver(bootstrapper.getDatabaseManagementService());
        URI uri = resolver.resolveDependency(AbstractNeoWebServer.class).getBaseUri();
        bootstrapper.stop();

        List<String> captured = readLines(
                testDirectory.getFileSystem(),
                testDirectory.homePath().resolve(LogConfig.USER_LOG),
                EmptyMemoryTracker.INSTANCE);
        assertThat(captured)
                .satisfies(containsAtLeastTheseLines(
                        warn("Config file \\[nonexistent-file.conf\\] does not exist."),
                        info("Starting..."),
                        info(NEO4J_IS_STARTING_MESSAGE),
                        info("Remote interface available at " + uri),
                        info("id: .*"),
                        info("name: system"),
                        info("creationDate: .*"),
                        info("Started."),
                        info("Stopping..."),
                        info("Stopped.")));
    }

    private static DependencyResolver getDependencyResolver(DatabaseManagementService managementService) {
        return ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
    }

    private Map<String, String> getPropertyPairs() {
        Map<String, String> properties = new HashMap<>();

        properties.put(
                GraphDatabaseSettings.data_directory.name(),
                testDirectory.homePath().toString());
        properties.put(
                GraphDatabaseSettings.logs_directory.name(),
                testDirectory.homePath().toString());

        properties.put(HttpConnector.listen_address.name(), "localhost:0");
        properties.put(HttpConnector.advertised_address.name(), ":0");
        properties.put(HttpConnector.enabled.name(), TRUE);

        properties.put(HttpsConnector.listen_address.name(), "localhost:0");
        properties.put(HttpsConnector.advertised_address.name(), ":0");
        properties.put(HttpsConnector.enabled.name(), FALSE);

        properties.put(BoltConnector.enabled.name(), TRUE);
        properties.put(BoltConnector.listen_address.name(), "localhost:0");
        properties.put(BoltConnector.advertised_address.name(), ":0");
        properties.put(BoltConnector.encryption_level.name(), "DISABLED");

        properties.put(
                GraphDatabaseInternalSettings.databases_root_path.name(),
                testDirectory.absolutePath().toString());
        return properties;
    }

    private static Condition<? super List<? extends String>> containsAtLeastTheseLines(
            Pattern... expectedLinePatterns) {
        return new Condition<>(
                lines -> {
                    if (expectedLinePatterns.length > lines.size()) {
                        return false;
                    }

                    for (int i = 0, e = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        while (!expectedLinePatterns[e].matcher(line).matches()) {
                            if (++i >= lines.size()) {
                                return false;
                            }
                            line = lines.get(i);
                        }
                        e++;
                    }
                    return true;
                },
                "Expected: " + asList(expectedLinePatterns));
    }

    private static Pattern info(String messagePattern) {
        return line("INFO", messagePattern);
    }

    private static Pattern warn(String messagePattern) {
        return line("WARN", messagePattern);
    }

    private static Pattern line(final String level, final String messagePattern) {
        return Pattern.compile(".*" + level + "\\s+" + messagePattern);
    }
}
