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

import java.io.IOException;

import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class RestartIT
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldBeAbleToReadExistingStoreOnRestart() throws IOException
    {
        // Given an existing store
        testDir.cleanup();
        var storeDir = testDir.absolutePath();
        var oldManagementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
        oldManagementService.shutdown();

        // When start with that store
        var managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();

        // Then should be able to access it
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        try ( var tx = db.beginTx() )
        {
            assertThat( tx.getAllNodes() ).isEmpty();
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
