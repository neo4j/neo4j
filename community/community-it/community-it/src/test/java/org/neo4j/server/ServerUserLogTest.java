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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.log4j.LogConfig.USER_LOG;
import static org.neo4j.server.NeoBootstrapper.OK;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
class ServerUserLogTest {
    @Inject
    private SuppressOutput suppress;

    @Inject
    private TestDirectory homeDir;

    @Test
    void shouldLogToStdOutWhenConfigured() throws IOException {
        // given
        NeoBootstrapper neoBootstrapper = getServerBootstrapper();
        Path dir = homeDir.homePath();
        InternalLog logBeforeStart = neoBootstrapper.getLog();
        Path xmlConfig = dir.resolve("neo4j.xml");

        // when
        try {
            String xml =
                    """
                    <Configuration packages="org.neo4j.logging.log4j">
                        <Appenders>
                            <Console name="console" target="SYSTEM_OUT" follow="true">
                                <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                            </Console>
                        </Appenders>
                        <Loggers>
                            <Root level="info">
                                <AppenderRef ref="console"/>
                            </Root>
                        </Loggers>
                    </Configuration>
                    """;
            FileSystemUtils.writeString(homeDir.getFileSystem(), xmlConfig, xml, EmptyMemoryTracker.INSTANCE);
            Map<String, String> configOverrides =
                    stringMap(GraphDatabaseSettings.user_logging_config_path.name(), xmlConfig.toString());
            configOverrides.putAll(connectorsConfig());

            int returnCode = neoBootstrapper.start(dir, null, configOverrides, false, false);

            // then no exceptions are thrown and
            assertThat(getStdOut()).isNotEmpty();
            assertThat(getUserLogFileLocation(dir)).doesNotExist();

            // then no exceptions are thrown and
            assertEquals(OK, returnCode);
            assertTrue(neoBootstrapper.isRunning());
            assertThat(neoBootstrapper.getLog()).isNotSameAs(logBeforeStart);

            assertThat(getStdOut()).isNotEmpty();
            assertThat(getStdOut()).anyMatch(s -> s.contains("Started."));
        } finally {
            // stop the server so that resources are released and test teardown isn't flaky
            neoBootstrapper.stop();
        }
        assertThat(getUserLogFileLocation(dir)).doesNotExist();
    }

    @Test
    void shouldLogToFileAndConsoleByDefault() throws Exception {
        // given
        NeoBootstrapper neoBootstrapper = getServerBootstrapper();
        Path dir = homeDir.homePath();
        InternalLog logBeforeStart = neoBootstrapper.getLog();

        // when
        try {
            int returnCode = neoBootstrapper.start(dir, connectorsConfig());
            // then no exceptions are thrown and
            assertEquals(OK, returnCode);
            assertTrue(neoBootstrapper.isRunning());
            assertThat(neoBootstrapper.getLog()).isNotSameAs(logBeforeStart);

        } finally {
            // stop the server so that resources are released and test teardown isn't flaky
            neoBootstrapper.stop();
        }
        assertThat(getStdOut()).isNotEmpty();
        assertThat(getUserLogFileLocation(dir)).exists();
        assertThat(readUserLogFile(dir)).isNotEmpty();
        assertThat(readUserLogFile(dir)).anyMatch(s -> s.contains("Started."));
    }

    private static Map<String, String> connectorsConfig() {
        return Map.of(
                HttpConnector.listen_address.name(),
                "localhost:0",
                BoltConnector.listen_address.name(),
                "localhost:0",
                HttpsConnector.listen_address.name(),
                "localhost:0",
                HttpConnector.advertised_address.name(),
                ":0",
                BoltConnector.advertised_address.name(),
                ":0",
                HttpsConnector.advertised_address.name(),
                ":0",
                GraphDatabaseSettings.preallocate_logical_logs.name(),
                FALSE);
    }

    private List<String> getStdOut() {
        List<String> lines = suppress.getOutputVoice().lines();
        // Remove empty lines
        return lines.stream().filter(line -> !line.equals("")).collect(Collectors.toList());
    }

    private static NeoBootstrapper getServerBootstrapper() {
        return new CommunityBootstrapper();
    }

    private static List<String> readUserLogFile(Path homeDir) throws IOException {
        return Files.readAllLines(getUserLogFileLocation(homeDir)).stream()
                .filter(line -> !line.equals(""))
                .collect(Collectors.toList());
    }

    private static Path getUserLogFileLocation(Path homeDir) {
        return homeDir.resolve("logs").resolve(USER_LOG);
    }
}
