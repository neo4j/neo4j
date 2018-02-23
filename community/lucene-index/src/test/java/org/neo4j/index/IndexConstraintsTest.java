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
package org.neo4j.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.helpers.collection.Iterables.single;

public class IndexConstraintsTest
{
    private static final Label LABEL = Label.label( "Label" );
    private static final String PROPERTY_KEY = "x";

    private GraphDatabaseService graphDb;

    @BeforeEach
    public void setup()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterEach
    public void shutdown()
    {
        graphDb.shutdown();
    }

    @Test
    public void testMultipleCreate() throws InterruptedException
    {
        final int numThreads = 25;
        final String uuid = UUID.randomUUID().toString();

        final Node commonNode;
        try ( Transaction tx = graphDb.beginTx() )
        {
            commonNode = graphDb.createNode();
            tx.success();
        }

        ExecutorCompletionService<Node> ecs = new ExecutorCompletionService<>(
                Executors.newFixedThreadPool( numThreads ) );
        for ( int i = 0; i < numThreads; i++ )
        {
            ecs.submit( () ->
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    final Node node = graphDb.createNode();
                    // Acquire lock
                    tx.acquireWriteLock( commonNode );
                    Index<Node> index = graphDb.index().forNodes( "uuids" );
                    final Node existing = index.get( "uuid", uuid ).getSingle();
                    if ( existing != null )
                    {
                        throw new RuntimeException( "Node already exists" );
                    }
                    node.setProperty( "uuid", uuid );
                    index.add( node, "uuid", uuid );
                    tx.success();
                    return node;
                }
            } );
        }
        int numSucceeded = 0;
        for ( int i = 0; i < numThreads; i++ )
        {
            try
            {
                ecs.take().get();
                ++numSucceeded;
            }
            catch ( ExecutionException ignored )
            {
            }
        }
        assertEquals( 1, numSucceeded );
    }

    // The following tests verify that multiple interacting schema commands can be applied in the same transaction.

    @Test
    public void convertIndexToConstraint()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            IndexDefinition index = firstOrNull( graphDb.schema().getIndexes( LABEL ) );
            index.drop();

            graphDb.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }
        // assert no exception is thrown
    }

    @Test
    public void convertIndexToConstraintWithExistingData()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < 2000; i++ )
            {
                Node node = graphDb.createNode( LABEL );
                node.setProperty( PROPERTY_KEY, i );
            }
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            IndexDefinition index = firstOrNull( graphDb.schema().getIndexes( LABEL ) );
            index.drop();

            graphDb.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }
        // assert no exception is thrown
    }

    @Test
    public void convertConstraintToIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            ConstraintDefinition constraint = firstOrNull( graphDb.schema().getConstraints( LABEL ) );
            constraint.drop();

            graphDb.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.success();
        }
        // assert no exception is thrown
    }

    @Test
    public void creatingAndDroppingAndCreatingIndexInSameTransaction()
    {
        // go increasingly meaner
        for ( int times = 1; times <= 4; times++ )
        {
            try
            {
                // when: CREATE, DROP, CREATE => effect: CREATE
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( graphDb.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create(), times );
                    tx.success();
                }
                // then
                assertNotNull( getIndex( LABEL, PROPERTY_KEY ), "Index should exist" );

                // when: DROP, CREATE => effect: <none>
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( getIndex( LABEL, PROPERTY_KEY ), times );
                    tx.success();
                }
                // then
                assertNotNull( getIndex( LABEL, PROPERTY_KEY ), "Index should exist" );

                // when: DROP, CREATE, DROP => effect: DROP
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( getIndex( LABEL, PROPERTY_KEY ), times )
                            .drop();
                    tx.success();
                }
                // then
                assertNull( getIndex( LABEL, PROPERTY_KEY ), "Index should be removed" );
            }
            catch ( Throwable e )
            {
                throw new AssertionError( "times=" + times, e );
            }
        }
    }

    private IndexDefinition recreate( IndexDefinition index, int times )
    {
        for ( int i = 0; i < times; i++ )
        {
            index.drop();
            index = graphDb.schema()
                    .indexFor( index.getLabel() )
                    .on( single( index.getPropertyKeys() ) )
                    .create();
        }
        return index;
    }

    private IndexDefinition getIndex( Label label, String propertyKey )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            IndexDefinition found = null;
            for ( IndexDefinition index : graphDb.schema().getIndexes( label ) )
            {
                if ( propertyKey.equals( single( index.getPropertyKeys() ) ) )
                {
                    assertNull( found, "Found multiple indexes." );
                    found = index;
                }
            }
            tx.success();
            return found;
        }
    }

    @Test
    public void shouldRemoveIndexForConstraintEvenIfDroppedInCreatingTransaction()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // given
            graphDb.schema()
                    .constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY )
                    .create()
                    .drop();
            // when - rolling back
            tx.failure();
        }
        // then
        assertNull( getIndex( LABEL, PROPERTY_KEY ), "Should not have constraint index" );
    }

    @Test
    public void creatingAndDroppingAndCreatingConstraintInSameTransaction()
    {
        // go increasingly meaner
        for ( int times = 1; times <= 4; times++ )
        {
            try
            {
                // when: CREATE, DROP, CREATE => effect: CREATE
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( graphDb.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create(), times );
                    tx.success();
                }
                // then
                assertNotNull( getConstraint( LABEL, PROPERTY_KEY ), "Constraint should exist" );
                assertNotNull( getIndex( LABEL, PROPERTY_KEY ), "Should have constraint index" );

                // when: DROP, CREATE => effect: <none>
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( getConstraint( LABEL, PROPERTY_KEY ), times );
                    tx.success();
                }
                // then
                assertNotNull( getConstraint( LABEL, PROPERTY_KEY ), "Constraint should exist" );
                assertNotNull( getIndex( LABEL, PROPERTY_KEY ), "Should have constraint index" );

                // when: DROP, CREATE, DROP => effect: DROP
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( getConstraint( LABEL, PROPERTY_KEY ), times )
                            .drop();
                    tx.success();
                }
                // then
                assertNull( getConstraint( LABEL, PROPERTY_KEY ), "Constraint should be removed" );
                assertNull( getIndex( LABEL, PROPERTY_KEY ), "Should not have constraint index" );
            }
            catch ( Throwable e )
            {
                throw new AssertionError( "times=" + times, e );
            }
        }
    }

    private ConstraintDefinition recreate( ConstraintDefinition constraint, int times )
    {
        for ( int i = 0; i < times; i++ )
        {
            constraint.drop();
            constraint = graphDb.schema()
                    .constraintFor( constraint.getLabel() )
                    .assertPropertyIsUnique( single( constraint.getPropertyKeys() ) )
                    .create();
        }
        return constraint;
    }

    private ConstraintDefinition getConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ConstraintDefinition found = null;
            for ( ConstraintDefinition constraint : graphDb.schema().getConstraints( label ) )
            {
                if ( propertyKey.equals( single( constraint.getPropertyKeys() ) ) )
                {
                    assertNull( found, "Found multiple constraints." );
                    found = constraint;
                }
            }
            tx.success();
            return found;
        }
    }
}
