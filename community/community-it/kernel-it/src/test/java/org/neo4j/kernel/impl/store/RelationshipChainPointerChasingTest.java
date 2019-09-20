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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.asArray;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.MyRelTypes.TEST2;
import static org.neo4j.kernel.impl.MyRelTypes.TEST_TRAVERSAL;

/**
 * Traversing a relationship chain has no consistency guarantees that there will be no change between
 * starting the traversal and continuing through it. Therefore relationships might get deleted right
 * when, or before, traversing there. The previous behaviour was to be aware of that and simply abort
 * the chain traversal.
 *
 * Given the fact that relationship ids will not be reused within the same database session the
 * behaviour has been changed to continue through such unused relationships, reading its pointers,
 * until arriving at either {@code -1} or a used relationship.
 */
@ImpermanentDbmsExtension( configurationCallback = "configure" )
class RelationshipChainPointerChasingTest
{
    private static final int THRESHOLD = 10;

    @Inject
    private GraphDatabaseService db;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.dense_node_threshold, THRESHOLD );
    }

    @Test
    void shouldChaseTheLivingRelationships() throws Exception
    {
        // GIVEN a sound relationship chain
        int numberOfRelationships = THRESHOLD / 2;
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                node.createRelationshipTo( tx.createNode(), TEST );
            }
            tx.commit();
        }
        Relationship[] relationships;
        try ( Transaction tx = db.beginTx() )
        {
            relationships = asArray( Relationship.class, tx.getNodeById( node.getId() ).getRelationships() );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node = tx.getNodeById( node.getId() );
            // WHEN getting the relationship iterator, i.e. starting to traverse this relationship chain,
            // the cursor eagerly goes to the first relationship before we call #hasNexxt/#next.
            Iterator<Relationship> iterator = node.getRelationships().iterator();

            // Therefore we delete relationships [1] and [2] (the second and third), since currently
            // the relationship iterator has read [0] and have already decided to go to [1] after our next
            // call to #next
            deleteRelationshipsInSeparateThread( relationships[1], relationships[2] );

            // THEN the relationship iterator should recognize the unused relationship, but still try to find
            // the next used relationship in this chain by following the pointers in the unused records.
            assertNext( relationships[0], iterator );
            assertNext( relationships[3], iterator );
            assertNext( relationships[4], iterator );
            assertFalse( iterator.hasNext() );
            tx.commit();
        }
    }

    @Test
    void shouldChaseTheLivingRelationshipGroups() throws Exception
    {
        // GIVEN
        Node node;
        Relationship relationshipInTheMiddle;
        Relationship relationshipInTheEnd;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            for ( int i = 0; i < THRESHOLD; i++ )
            {
                node.createRelationshipTo( tx.createNode(), TEST );
            }
            relationshipInTheMiddle = node.createRelationshipTo( tx.createNode(), TEST2 );
            relationshipInTheEnd = node.createRelationshipTo( tx.createNode(), TEST_TRAVERSAL );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node = tx.getNodeById( node.getId() );
            relationshipInTheEnd = tx.getRelationshipById( relationshipInTheEnd.getId() );

            // WHEN getting the relationship iterator, the first group record will be read and held,
            // already pointing to the next group
            Iterator<Relationship> relationships = node.getRelationships().iterator();
            for ( int i = 0; i < THRESHOLD / 2; i++ )
            {
                assertTrue( relationships.next().isType( TEST ) );
            }

            // Here we're awfully certain that we're on this first group, so we go ahead and delete the
            // next one in a simulated concurrent transaction in another thread
            deleteRelationshipsInSeparateThread( relationshipInTheMiddle );

            // THEN we should be able to, first of all, iterate through the rest of the relationships of the first type
            for ( int i = 0; i < THRESHOLD / 2; i++ )
            {
                assertTrue( relationships.next().isType( TEST ) );
            }
            // THEN we should be able to see the last relationship, after the deleted one
            // where the group for the deleted relationship also should've been deleted since it was the
            // only on of that type.
            assertNext( relationshipInTheEnd, relationships );
            assertFalse( relationships.hasNext() );

            tx.commit();
        }
    }

    private static void assertNext( Relationship expected, Iterator<Relationship> iterator )
    {
        assertTrue( iterator.hasNext(), "Expected there to be more relationships" );
        assertEquals( expected, iterator.next(), "Unexpected next relationship" );
    }

    private void deleteRelationshipsInSeparateThread( final Relationship... relationships ) throws InterruptedException
    {
        executeTransactionInSeparateThread( tx ->
        {
            for ( Relationship relationship : relationships )
            {
                tx.getRelationshipById( relationship.getId() ).delete();
            }
        } );
    }

    private void executeTransactionInSeparateThread( final Consumer<Transaction> actionInsideTransaction ) throws InterruptedException
    {
        Thread thread = new Thread( () -> {
            try ( Transaction tx = db.beginTx() )
            {
                actionInsideTransaction.accept( tx );
                tx.commit();
            }
        } );
        thread.start();
        thread.join();
    }
}
