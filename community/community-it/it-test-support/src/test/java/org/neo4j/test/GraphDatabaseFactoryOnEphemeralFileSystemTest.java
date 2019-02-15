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
package org.neo4j.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
public class GraphDatabaseFactoryOnEphemeralFileSystemTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;

    private GraphDatabaseService db;

    @BeforeEach
    void createDb()
    {
        db = createGraphDatabaseFactory().setFileSystem( fs ).newEmbeddedDatabaseBuilder( dir.storeDir() ).newGraphDatabase();
    }

    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    @AfterEach
    void tearDown()
    {
        db.shutdown();
    }

    @Test
    void shouldKeepDataBetweenStartAndShutdown()
    {
        createNode();

        assertEquals( 1, nodeCount(), "Expected one new node" );
    }

    @Test
    void dataShouldNotSurviveRestartOnSameFileSystem()
    {
        createNode();
        db.shutdown(); // Closing the ephemeral file system deletes all of its data.

        createDb();

        assertEquals( 0, nodeCount(), "Should not see anything." );
    }

    @Test
    void dataCreatedAfterCrashShouldNotSurvive()
    {
        fs = fs.snapshot(); // Crash before we create any data.

        createNode(); // Pretend to create data, but we are post-crash, so the database should never see this.
        db.shutdown();
        createDb(); // Start database up on the crash snapshot.

        assertEquals( 0, nodeCount(), "Should not see anything." );
    }

    @Test
    void shouldRemoveAllData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipType relationshipType = RelationshipType.withName( "R" );

            Node n1 = db.createNode();
            Node n2 = db.createNode();
            Node n3 = db.createNode();

            n1.createRelationshipTo(n2, relationshipType);
            n2.createRelationshipTo(n1, relationshipType);
            n3.createRelationshipTo(n1, relationshipType);

            tx.success();
        }

        cleanDatabaseContent( db );

        assertThat( nodeCount(), is( 0L ) );
    }

    private void cleanDatabaseContent( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.getAllRelationships().forEach( Relationship::delete );
            db.getAllNodes().forEach( Node::delete );
            tx.success();
        }
    }

    private long nodeCount()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return Iterables.count( db.getAllNodes() );
        }
    }

    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }
}
