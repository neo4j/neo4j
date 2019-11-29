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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.LogService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class GraphDatabaseInternalLogIT
{
    private static final String INTERNAL_LOG_FILE = "debug.log";
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldWriteToInternalDiagnosticsLog() throws Exception
    {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDir.homeDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").toPath().toAbsolutePath() )
                .build();
        managementService.shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile() ).isEqualTo( true );
        assertThat( internalLog.length() ).isGreaterThan( 0L );

        assertEquals( 1, countOccurrences( internalLog, "Database " + DEFAULT_DATABASE_NAME + " is ready." ) );
        assertEquals( 2, countOccurrences( internalLog, "Database " + DEFAULT_DATABASE_NAME + " is unavailable." ) );
    }

    @Test
    void shouldNotWriteDebugToInternalDiagnosticsLogByDefault() throws Exception
    {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDir.homeDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").toPath().toAbsolutePath() )
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogService.class );
        logService.getInternalLog( getClass() ).debug( "A debug entry" );

        managementService.shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile() ).isEqualTo( true );
        assertThat( internalLog.length() ).isGreaterThan( 0L );

        assertEquals( 0, countOccurrences( internalLog, "A debug entry" ) );
    }

    @Test
    void shouldWriteDebugToInternalDiagnosticsLogForEnabledContexts() throws Exception
    {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDir.homeDir() )
                .setConfig( GraphDatabaseSettings.store_internal_debug_contexts, List.of( getClass().getName(), "java.io" ) )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").toPath().toAbsolutePath() )
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogService.class );
        logService.getInternalLog( getClass() ).debug( "A debug entry" );
        logService.getInternalLog( GraphDatabaseService.class ).debug( "A GDS debug entry" );
        logService.getInternalLog( StringWriter.class ).debug( "A SW debug entry" );

        managementService.shutdown();
        File internalLog = new File( testDir.directory( "logs" ), INTERNAL_LOG_FILE );

        // Then
        assertThat( internalLog.isFile() ).isEqualTo( true );
        assertThat( internalLog.length() ).isGreaterThan( 0L );

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
