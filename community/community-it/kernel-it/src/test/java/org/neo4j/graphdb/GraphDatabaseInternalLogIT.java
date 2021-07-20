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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.LogService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

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
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDir.homePath() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory( "logs" ).toAbsolutePath() )
                .build();

        var databaseId = ((GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME )).databaseId();
        managementService.shutdown();
        Path internalLog = testDir.directory( "logs" ).resolve( INTERNAL_LOG_FILE );

        // Then
        assertThat( Files.isRegularFile( internalLog ) ).isEqualTo( true );
        assertThat( Files.size( internalLog ) ).isGreaterThan( 0L );

        assertEquals( 1, countOccurrences( internalLog, databaseId + " is ready." ) );
        assertEquals( 2, countOccurrences( internalLog, databaseId + " is unavailable." ) );
    }

    @Test
    void shouldNotWriteDebugToInternalDiagnosticsLogByDefault() throws Exception
    {
        // Given
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDir.homePath() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory("logs").toAbsolutePath() )
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        // When
        LogService logService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogService.class );
        logService.getInternalLog( getClass() ).debug( "A debug entry" );

        managementService.shutdown();
        Path internalLog = testDir.directory( "logs" ).resolve( INTERNAL_LOG_FILE );

        // Then
        assertThat( Files.isRegularFile( internalLog ) ).isEqualTo( true );
        assertThat( Files.size( internalLog ) ).isGreaterThan( 0L );

        assertEquals( 0, countOccurrences( internalLog, "A debug entry" ) );
    }

    private static long countOccurrences( Path file, String substring ) throws IOException
    {
        try ( Stream<String> lines = Files.lines( file ) )
        {
            return lines.filter( line -> line.contains( substring ) ).count();
        }
    }
}
