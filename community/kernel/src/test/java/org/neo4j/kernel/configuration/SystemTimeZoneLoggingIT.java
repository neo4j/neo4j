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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class SystemTimeZoneLoggingIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void databaseLogsUseSystemTimeZoneIfConfigure() throws IOException
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
        File storeDir = testDirectory.directory( String.valueOf( hoursShift ) );
        GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM.name() ).newGraphDatabase();
        database.shutdown();
        Path databasePath = storeDir.toPath();
        Path debugLog = Paths.get( "logs", "debug.log" );
        String debugLogLine = getLogLine( databasePath, debugLog );
        assertTrue( debugLogLine, debugLogLine.contains( timeZoneSuffix ) );
    }

    private String getLogLine( Path databasePath, Path logFilePath ) throws IOException
    {
        return Files.readAllLines( databasePath.resolve( logFilePath ) ).get( 0 );
    }
}
