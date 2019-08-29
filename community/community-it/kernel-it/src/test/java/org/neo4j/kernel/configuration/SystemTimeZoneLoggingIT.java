/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.configuration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class SystemTimeZoneLoggingIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void databaseLogsUseSystemTimeZoneIfConfigure() throws IOException
    {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            checkStartLogLine( 5, "+0500" );
            checkStartLogLine( -7, "-0700" );
        }
        finally
        {
            TimeZone.setDefault( defaultTimeZone );
        }
    }

    private void checkStartLogLine( int hoursShift, String timeZoneSuffix ) throws IOException
    {
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( hoursShift ) ) );
        File storeDir = testDirectory.storeDir( String.valueOf( hoursShift ) );
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( storeDir )
                        .setConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
        Path databasePath = storeDir.toPath();
        Path debugLog = Paths.get( "logs", "debug.log" );
        String debugLogLine = getLogLine( databasePath, debugLog );
        assertTrue( debugLogLine.contains( timeZoneSuffix ), debugLogLine );
    }

    private static String getLogLine( Path databasePath, Path logFilePath ) throws IOException
    {
        return Files.readAllLines( databasePath.resolve( logFilePath ) ).get( 0 );
    }
}
