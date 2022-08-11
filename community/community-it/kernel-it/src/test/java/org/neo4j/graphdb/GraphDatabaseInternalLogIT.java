/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.log4j.LogConfig.DEBUG_LOG;
import static org.neo4j.logging.log4j.LogConfig.SERVER_LOGS_XML;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class GraphDatabaseInternalLogIT {

    @Inject
    private TestDirectory testDir;

    @Test
    void shouldWriteToInternalDiagnosticsLog() throws Exception {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(testDir.homePath())
                .setConfig(
                        GraphDatabaseSettings.logs_directory,
                        testDir.directory("logs").toAbsolutePath())
                .build();

        var databaseId = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME)).databaseId();
        managementService.shutdown();
        Path internalLog = testDir.directory("logs").resolve(DEBUG_LOG);

        // Then
        assertThat(Files.isRegularFile(internalLog)).isEqualTo(true);
        assertThat(Files.size(internalLog)).isGreaterThan(0L);

        assertEquals(1, countOccurrences(internalLog, databaseId + " is ready."));
        assertEquals(2, countOccurrences(internalLog, databaseId + " is unavailable."));
    }

    @Test
    void shouldNotWriteDebugToInternalDiagnosticsLogByDefault() throws Exception {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(testDir.homePath())
                .setConfig(
                        GraphDatabaseSettings.logs_directory,
                        testDir.directory("logs").toAbsolutePath())
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(LogService.class);
        logService.getInternalLog(getClass()).debug("A debug entry");

        managementService.shutdown();
        Path internalLog = testDir.directory("logs").resolve(DEBUG_LOG);

        // Then
        assertThat(Files.isRegularFile(internalLog)).isEqualTo(true);
        assertThat(Files.size(internalLog)).isGreaterThan(0L);

        assertEquals(0, countOccurrences(internalLog, "A debug entry"));
    }

    @Test
    void shouldUseXmlConfigurationIfPresent() throws IOException {
        // Given
        Path log4jXmlConfig = testDir.homePath().resolve(SERVER_LOGS_XML);
        writeResourceToFile("testConfig.xml", log4jXmlConfig);
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(testDir.homePath())
                .setConfig(GraphDatabaseSettings.server_logging_config_path, log4jXmlConfig)
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(LogService.class);
        logService.getInternalLog(getClass()).info("An info entry");

        managementService.shutdown();
        Path internalLog = testDir.directory("logs").resolve("debug.log");
        Path internalLog2 = testDir.directory("logs").resolve("debug2.log");

        // Then
        assertThat(internalLog).isRegularFile();
        assertThat(Files.size(internalLog)).isGreaterThan(0L);
        assertThat(internalLog2).isRegularFile();
        assertThat(Files.size(internalLog2)).isGreaterThan(0L);

        assertEquals(1, countOccurrences(internalLog, "An info entry"));
        assertEquals(1, countOccurrencesJson(internalLog2, "message", "An info entry"));
    }

    @Test
    @Timeout(value = 1, unit = MINUTES)
    void shouldHandleReconfiguringOfXmlConfiguration() throws IOException, InterruptedException {
        // Given
        Path log4jXmlConfig = testDir.homePath().resolve(SERVER_LOGS_XML);
        Files.createDirectories(log4jXmlConfig.getParent());
        writeResourceToFile("testConfig2.xml", log4jXmlConfig);
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(testDir.homePath())
                .setConfig(GraphDatabaseSettings.server_logging_config_path, log4jXmlConfig)
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

        InternalLog log = ((GraphDatabaseAPI) db)
                .getDependencyResolver()
                .resolveDependency(LogService.class)
                .getInternalLog(getClass());
        log.info("An info entry");

        Path internalLog = testDir.directory("logs").resolve("debug.log");
        Path internalLog2 = testDir.directory("logs").resolve("debug2.log");
        assertThat(internalLog).isRegularFile();
        assertEquals(1, countOccurrences(internalLog, "An info entry"));
        assertThat(internalLog2).doesNotExist();

        // Overwrite
        writeResourceToFile("testConfig.xml", log4jXmlConfig);

        // Wait for log4j to pick up on the changes
        do {
            log.info("An info entry");
            Thread.sleep(100);
        } while (!Files.exists(internalLog2));

        log.info("An info entry");
        managementService.shutdown();

        assertThat(internalLog).isRegularFile();
        assertThat(internalLog2).isRegularFile();
        assertThat(countOccurrences(internalLog, "An info entry")).isGreaterThan(0);
        assertThat(countOccurrencesJson(internalLog2, "message", "An info entry"))
                .isGreaterThan(0);
    }

    private static long countOccurrences(Path file, String substring) throws IOException {
        try (Stream<String> lines = Files.lines(file)) {
            return lines.filter(line -> line.contains(substring)).count();
        }
    }

    private void writeResourceToFile(String name, Path log4jXmlConfig) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(name)) {
            Files.copy(requireNonNull(in), log4jXmlConfig, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static long countOccurrencesJson(Path file, String key, String substring) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (Stream<String> lines = Files.lines(file)) {
            return lines.filter(line -> {
                        try {
                            JsonNode logLine = mapper.readTree(line);
                            String value = logLine.get(key).asText();
                            return value.contains(substring);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .count();
        }
    }
}
