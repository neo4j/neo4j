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
package org.neo4j.dbms.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.noOpSystemGraphInitializer;

@Neo4jLayoutExtension
class DatabaseFailureIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private Neo4jLayout neo4jLayout;
    private GraphDatabaseAPI database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        database = startDatabase();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void startWhenDefaultDatabaseFailedToStart() throws IOException
    {
        managementService.shutdown();
        deleteDirectory( databaseLayout.getTransactionLogsDirectory() );

        database = startDatabase();
        DatabaseStateService databaseStateService = database.getDependencyResolver().resolveDependency( DatabaseStateService.class );
        assertTrue( databaseStateService.causeOfFailure( database.databaseId() ).isPresent() );
        assertFalse( databaseStateService.causeOfFailure( NAMED_SYSTEM_DATABASE_ID ).isPresent() );
    }

    @Test
    void failToStartWhenSystemDatabaseFailedToStart() throws IOException
    {
        managementService.shutdown();
        deleteDirectory( neo4jLayout.databaseLayout( SYSTEM_DATABASE_NAME ).getTransactionLogsDirectory() );

        Exception startException = assertThrows( Exception.class, this::startDatabase );
        assertThat( startException ).hasCauseInstanceOf( UnableToStartDatabaseException.class );
    }

    private GraphDatabaseAPI startDatabase()
    {
        startDatabaseServer();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void startDatabaseServer()
    {
        managementService = new DatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setExternalDependencies( noOpSystemGraphInitializer() )
                .build();
    }
}
