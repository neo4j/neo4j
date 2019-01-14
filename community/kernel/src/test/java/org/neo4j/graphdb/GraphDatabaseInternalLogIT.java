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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.stream.Stream;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

public class GraphDatabaseInternalLogIT
{
    public static final String INTERNAL_LOG_FILE = "debug.log";
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void shouldWriteToInternalDiagnosticsLog() throws Exception
    {
        // Given
        new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").getAbsolutePath() )
                .newGraphDatabase().shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile(), is( true ) );
        assertThat( internalLog.length(), greaterThan( 0L ) );

        assertEquals( 1, countOccurrences( internalLog, "Database is now ready" ) );
        assertEquals( 1, countOccurrences( internalLog, "Database is now unavailable" ) );
    }

    @Test
    public void shouldNotWriteDebugToInternalDiagnosticsLogByDefault() throws Exception
    {
        // Given
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").getAbsolutePath() )
                .newGraphDatabase();

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogService.class );
        logService.getInternalLog( getClass() ).debug( "A debug entry" );

        db.shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile(), is( true ) );
        assertThat( internalLog.length(), greaterThan( 0L ) );

        assertEquals( 0, countOccurrences( internalLog, "A debug entry" ) );
    }

    @Test
    public void shouldWriteDebugToInternalDiagnosticsLogForEnabledContexts() throws Exception
    {
        // Given
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                .setConfig( GraphDatabaseSettings.store_internal_debug_contexts, getClass().getName() + ",java.io" )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").getAbsolutePath() )
                .newGraphDatabase();

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogService.class );
        logService.getInternalLog( getClass() ).debug( "A debug entry" );
        logService.getInternalLog( GraphDatabaseService.class ).debug( "A GDS debug entry" );
        logService.getInternalLog( StringWriter.class ).debug( "A SW debug entry" );

        db.shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile(), is( true ) );
        assertThat( internalLog.length(), greaterThan( 0L ) );

        assertEquals( 1, countOccurrences( internalLog, "A debug entry" ) );
        assertEquals( 0, countOccurrences( internalLog, "A GDS debug entry" ) );
        assertEquals( 1, countOccurrences( internalLog, "A SW debug entry" ) );
    }

    private static long countOccurrences( File file, String substring ) throws IOException
    {
        try ( Stream<String> lines = Files.lines( file.toPath() ) )
        {
            return lines.filter( line -> line.contains( substring ) ).count();
        }
    }
}
