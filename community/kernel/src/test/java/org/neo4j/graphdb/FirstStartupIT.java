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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import javax.annotation.Resource;

import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

@ExtendWith( TestDirectoryExtension.class )
public class FirstStartupIT
{
    @Resource
    public TestDirectory testDir;

    @Test
    public void shouldBeEmptyWhenFirstStarted()
    {
        // When
        File storeDir = testDir.absolutePath();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            assertEquals( 0, count( db.getAllNodes() ) );
            assertEquals( 0, count( db.getAllRelationships() ) );
            assertEquals( 0, count( db.getAllRelationshipTypes() ) );
            assertEquals( 0, count( db.getAllLabels() ) );
            assertEquals( 0, count( db.getAllPropertyKeys() ) );
        }

        db.shutdown();
    }
}
