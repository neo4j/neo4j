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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.count;

@TestDirectoryExtension
class FirstStartupIT
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldBeEmptyWhenFirstStarted()
    {
        // When
        File storeDir = testDir.absolutePath();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        // Then
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( 0, count( transaction.getAllNodes() ) );
            assertEquals( 0, count( transaction.getAllRelationships() ) );
            assertEquals( 0, count( transaction.getAllRelationshipTypes() ) );
            assertEquals( 0, count( transaction.getAllLabels() ) );
            assertEquals( 0, count( transaction.getAllPropertyKeys() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
