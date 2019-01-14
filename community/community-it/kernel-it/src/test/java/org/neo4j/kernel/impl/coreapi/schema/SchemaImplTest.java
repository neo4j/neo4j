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
package org.neo4j.kernel.impl.coreapi.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
public class SchemaImplTest
{
    private static final Label USER_LABEL = Label.label( "User" );

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;

    @BeforeEach
    void createDb()
    {
        db = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase( testDirectory.databaseDir() );
    }

    @AfterEach
    void shutdownDb()
    {
        db.shutdown();
    }

    @Test
    void testGetIndexPopulationProgress() throws Exception
    {
        assertFalse( indexExists( USER_LABEL ) );

        // Create some nodes
        try ( Transaction tx = db.beginTx() )
        {
            Label label = Label.label( "User" );

            // Create a huge bunch of users so the index takes a while to build
            for ( int id = 0; id < 100000; id++ )
            {
                Node userNode = db.createNode( label );
                userNode.setProperty( "username", "user" + id + "@neo4j.org" );
            }
            tx.success();
        }

        // Create an index
        IndexDefinition indexDefinition;
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = db.schema();
            indexDefinition = schema.indexFor( USER_LABEL ).on( "username" ).create();
            tx.success();
        }

        // Get state and progress
        try ( Transaction ignore = db.beginTx() )
        {
            Schema schema = db.schema();
            Schema.IndexState state;

            IndexPopulationProgress progress;
            do
            {
                state = schema.getIndexState( indexDefinition );
                progress = schema.getIndexPopulationProgress( indexDefinition );

                assertTrue( progress.getCompletedPercentage() >= 0 );
                assertTrue( progress.getCompletedPercentage() <= 100 );
                Thread.sleep( 10 );
            }
            while ( state == Schema.IndexState.POPULATING );

            assertSame( state, Schema.IndexState.ONLINE );
            assertEquals( 100.0, progress.getCompletedPercentage(), 0.0001 );
        }
    }

    @Test
    void createdIndexDefinitionsMustBeUnnamed()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( USER_LABEL ).on( "name" ).create();
            assertThat( index.getName(), is( IndexReference.UNNAMED_INDEX ) );
            tx.success();
        }
    }

    @Test
    void mustRememberNamesOfCreatedIndex()
    {
        String indexName = "Users index";
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( USER_LABEL ).on( "name" ).withName( indexName ).create();
            assertThat( index.getName(), is( indexName ) );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().getIndexByName( indexName );
            assertThat( index.getName(), is( indexName ) );
            tx.success();
        }
    }

    private boolean indexExists( Label label )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Iterable<IndexDefinition> indexes = db.schema().getIndexes( label );
            IndexDefinition index = Iterables.firstOrNull( indexes );
            boolean exists = index != null;
            transaction.success();
            return exists;
        }
    }
}
