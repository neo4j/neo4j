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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Reconfigurable;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.configuration.Config;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
@TestDirectoryExtension
class LoggingSettingsMigratorTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private SuppressOutput suppressOutput;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private Path neo4jConfig;
    private Path userLogs;
    private Path serverLogs;
    private String migratedUserLogs;
    private String migratedServerLogs;
    private List<String> errorLines = new ArrayList<>();

    @BeforeEach
    void setUp() {
        neo4jConfig = testDirectory.file(Config.DEFAULT_CONFIG_FILE_NAME);
        userLogs = testDirectory.file(LogConfig.USER_LOGS_XML);
        serverLogs = testDirectory.file(LogConfig.SERVER_LOGS_XML);

        StatusLogger.getLogger().registerListener(new StatusListener() {
            @Override
            public void log(StatusData data) {
                errorLines.add(data.getFormattedStatus());
            }

            @Override
            public Level getStatusLevel() {
                return Level.ERROR;
            }

            @Override
            public void close() {}
        });
    }

    @Test
    void defaultValues() throws IOException {
        migrateConfig("");
        assertThat(migratedUserLogs.stripIndent())
                .contains(
                        """
                    <Appenders>
                        <RandomAccessFile name="Neo4jLog" fileName="${config:server.directories.logs}/neo4j.log">
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RandomAccessFile>

                        <!-- Only used by "neo4j console", will be ignored otherwise -->
                        <Console name="ConsoleAppender" target="SYSTEM_OUT">
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </Console>
                    </Appenders>

                    <Loggers>
                        <!-- Log level for the neo4j log. One of DEBUG, INFO, WARN, ERROR or OFF -->
                        <Root level="INFO">
                            <AppenderRef ref="Neo4jLog"/>
                            <AppenderRef ref="ConsoleAppender"/>
                        </Root>
                    </Loggers>
                """);
        assertThat(migratedServerLogs.stripIndent())
                .contains(
                        """
                    <Appenders>
                        <RollingRandomAccessFile name="DebugLog" fileName="${config:server.directories.logs}/debug.log"
                                filePattern="${config:server.directories.logs}/debug.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="20 MB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="7"/>
                            <Neo4jDebugLogLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p [%c{1.}] %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="HttpLog" fileName="${config:server.directories.logs}/http.log"
                                filePattern="${config:server.directories.logs}/http.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="20 MB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="5"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="QueryLog" fileName="${config:server.directories.logs}/query.log"
                                filePattern="${config:server.directories.logs}/query.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="20 MB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="7"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="SecurityLog" fileName="${config:server.directories.logs}/security.log"
                                filePattern="${config:server.directories.logs}/security.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="20 MB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="7"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                    </Appenders>

                    <Loggers>
                        <!-- Log levels. One of DEBUG, INFO, WARN, ERROR or OFF -->

                        <!-- The debug log is used as the root logger to catch everything -->
                        <Root level="INFO">
                            <AppenderRef ref="DebugLog"/> <!-- Keep this -->
                        </Root>

                        <!-- The query log, must be named "QueryLogger" -->
                        <Logger name="QueryLogger" level="INFO" additivity="false">
                            <AppenderRef ref="QueryLog"/>
                        </Logger>

                        <!-- The http request log, must be named "HttpLogger" -->
                        <Logger name="HttpLogger" level="INFO" additivity="false">
                            <AppenderRef ref="HttpLog"/>
                        </Logger>

                        <!-- The security log, must be named "SecurityLogger" -->
                        <Logger name="SecurityLogger" level="INFO" additivity="false">
                            <AppenderRef ref="SecurityLog"/>
                        </Logger>
                    </Loggers>
                """);
    }

    @Test
    void userLogsRotationSettings() throws IOException {
        migrateConfig(
                """
                dbms.logs.user.rotation.size=10K
                dbms.logs.user.rotation.keep_number=5
                """);
        assertThat(migratedUserLogs.stripIndent())
                .contains(
                        """
                            <Policies>
                                <SizeBasedTriggeringPolicy size="10 KB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="5"/>
                """);
    }

    @Test
    void serverLogsRotationSettings() throws IOException {
        migrateConfig(
                """
                dbms.logs.debug.rotation.size=10K
                dbms.logs.debug.rotation.keep_number=2
                dbms.logs.http.rotation.size=10M
                dbms.logs.http.rotation.keep_number=3
                dbms.logs.query.rotation.size=10G
                dbms.logs.query.rotation.keep_number=8
                dbms.logs.security.rotation.size=1024
                dbms.logs.security.rotation.keep_number=9
                """);
        assertThat(migratedServerLogs.stripIndent())
                .contains(
                        """
                        <RollingRandomAccessFile name="DebugLog" fileName="${config:server.directories.logs}/debug.log"
                                filePattern="${config:server.directories.logs}/debug.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="10 KB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="2"/>
                            <Neo4jDebugLogLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p [%c{1.}] %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="HttpLog" fileName="${config:server.directories.logs}/http.log"
                                filePattern="${config:server.directories.logs}/http.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="10 MB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="3"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="QueryLog" fileName="${config:server.directories.logs}/query.log"
                                filePattern="${config:server.directories.logs}/query.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="10 GB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="8"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                        <RollingRandomAccessFile name="SecurityLog" fileName="${config:server.directories.logs}/security.log"
                                filePattern="${config:server.directories.logs}/security.log.%02i">
                            <Policies>
                                <SizeBasedTriggeringPolicy size="1 KB"/>
                            </Policies>
                            <DefaultRolloverStrategy fileIndex="min" max="9"/>
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RollingRandomAccessFile>
                """);
    }

    @Test
    void serverLogsLevels() throws IOException {
        migrateConfig(
                """
                dbms.logs.debug.level=ERROR
                dbms.logs.security.level=WARN
                """);
        assertThat(migratedServerLogs).contains("<Root level=\"ERROR\">");
        assertThat(migratedServerLogs).contains("<Logger name=\"SecurityLogger\" level=\"WARN\" additivity=\"false\">");
    }

    @Test
    void relativePath() throws IOException {
        migrateConfig(
                """
                dbms.logs.user.path=dir/neo.log
                dbms.logs.debug.path=../../debug.log
                """);

        assertThat(migratedUserLogs).contains("fileName=\"${config:server.directories.logs}/dir/neo.log\"");
        assertThat(migratedServerLogs).contains("fileName=\"${config:server.directories.logs}/../../debug.log\"");
    }

    @Test
    void jsonFormat() throws IOException {
        migrateConfig(
                """
                # Set to 0 to get smaller output
                dbms.logs.user.rotation.size=0
                dbms.logs.debug.rotation.size=0
                dbms.logs.http.rotation.size=0
                dbms.logs.query.rotation.size=0
                dbms.logs.security.rotation.size=0

                dbms.logs.default_format=JSON
                dbms.logs.http.format=PLAIN
                """);
        assertThat(migratedUserLogs)
                .contains(
                        "<JsonTemplateLayout eventTemplateUri=\"classpath:org/neo4j/logging/StructuredLayoutWithMessage.json\"/>");
        assertThat(migratedServerLogs.stripIndent())
                .contains(
                        """
                        <RandomAccessFile name="DebugLog" fileName="${config:server.directories.logs}/debug.log">
                            <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredLayoutWithCategory.json"/>
                        </RandomAccessFile>
                        <RandomAccessFile name="HttpLog" fileName="${config:server.directories.logs}/http.log">
                            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSSZ}{GMT+0} %-5p %m%n"/>
                        </RandomAccessFile>
                        <RandomAccessFile name="QueryLog" fileName="${config:server.directories.logs}/query.log">
                            <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/QueryLogJsonLayout.json"/>
                        </RandomAccessFile>
                        <RandomAccessFile name="SecurityLog" fileName="${config:server.directories.logs}/security.log">
                            <JsonTemplateLayout eventTemplateUri="classpath:org/neo4j/logging/StructuredJsonLayout.json"/>
                        </RandomAccessFile>
                """);
    }

    @Test
    void preserveOldFiles() throws IOException {
        Files.writeString(userLogs, "Old user-logs.xml", StandardCharsets.UTF_8);
        Files.writeString(serverLogs, "Old server-logs.xml", StandardCharsets.UTF_8);
        migrateConfig("");
        Path oldUserLogs = userLogs.resolveSibling(userLogs.getFileName() + ".old");
        Path oldServerLogs = serverLogs.resolveSibling(serverLogs.getFileName() + ".old");
        assertThat(oldUserLogs).exists();
        assertThat(Files.readString(oldUserLogs, StandardCharsets.UTF_8)).contains("Old user-logs.xml");
        assertThat(oldServerLogs).exists();
        assertThat(Files.readString(oldServerLogs, StandardCharsets.UTF_8)).contains("Old server-logs.xml");

        assertThat(out.toString()).contains("Keeping original user-logs.xml file at");
        assertThat(out.toString()).contains("Keeping original server-logs.xml file at");
    }

    private void migrateConfig(String cfg) throws IOException {
        Files.writeString(neo4jConfig, cfg, StandardCharsets.UTF_8);
        new LoggingSettingsMigrator(
                        new PrintStream(out), neo4jConfig, ConfigFileMigrator.buildRawConfig(neo4jConfig, null))
                .migrate();
        migratedUserLogs = Files.readString(userLogs, StandardCharsets.UTF_8);
        migratedServerLogs = Files.readString(serverLogs, StandardCharsets.UTF_8);

        // Will parse xml files and write to System.err on errors
        new XmlConfigValidator(userLogs);
        new XmlConfigValidator(serverLogs);

        assertThat(errorLines).isEmpty();
        assertThat(suppressOutput.getOutputVoice().isEmpty()).isTrue();
        assertThat(suppressOutput.getErrorVoice().isEmpty()).isTrue();
    }

    private static class XmlConfigValidator extends XmlConfiguration {
        public XmlConfigValidator(Path configSource) {
            super(null, ConfigurationSource.fromUri(configSource.toUri()));
        }

        @Override
        protected void initializeWatchers(
                Reconfigurable reconfigurable, ConfigurationSource configSource, int monitorIntervalSeconds) {
            // Don't
        }
    }
}
