/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

public class FirstStartupIT
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldBeEmptyWhenFirstStarted() throws Exception
    {
        // When
        String storeDir = testDir.absolutePath();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // Then
        try(Transaction ignore = db.beginTx())
        {
            GlobalGraphOperations global = GlobalGraphOperations.at( db );

            assertEquals(0, count( global.getAllNodes() ));
            assertEquals(0, count( global.getAllRelationships() ));
            assertEquals(0, count( global.getAllRelationshipTypes() ));
            assertEquals(0, count( global.getAllLabels() ));
            assertEquals(0, count( global.getAllPropertyKeys() ));
        }

        db.shutdown();
    }
}
