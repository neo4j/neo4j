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
package org.neo4j.kernel.configuration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.log4j.LogConfig.DEBUG_LOG;
import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SystemTimeZoneLoggingIT {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void databaseLogsUseSystemTimeZoneIfConfigure() throws IOException {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            checkStartLogLine(5, "+0500");
            checkStartLogLine(-7, "-0700");
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    private void checkStartLogLine(int hoursShift, String timeZoneSuffix) throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.ofHours(hoursShift)));
        Path storeDir = testDirectory.homePath(String.valueOf(hoursShift));
        Path debugLog = storeDir.resolve("logs").resolve(DEBUG_LOG);
        Path logXmlConfig = storeDir.resolve("log.xml");
        newXmlConfigBuilder(testDirectory.getFileSystem(), logXmlConfig)
                .withLogger(newLoggerBuilder(ROOT_LOGGER, debugLog)
                        .withTimezone(LogTimeZone.SYSTEM)
                        .build())
                .create();

        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM)
                .setConfig(GraphDatabaseSettings.server_logging_config_path, logXmlConfig)
                .build();
        managementService.database(DEFAULT_DATABASE_NAME);
        managementService.shutdown();

        String debugLogLine = Files.readAllLines(debugLog).get(0);
        assertTrue(debugLogLine.contains(timeZoneSuffix), debugLogLine);
    }
}
