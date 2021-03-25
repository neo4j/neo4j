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
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( RandomExtension.class )
@ImpermanentDbmsExtension
class DeleteNodeWithRelationshipsIT
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldGiveHelpfulExceptionWhenDeletingNodeWithRelationships()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "MAYOR_OF" ) );
            tx.commit();
        }

        // And given a transaction deleting just the node
        Transaction tx = db.beginTx();
        tx.getNodeById( node.getId() ).delete();

        ConstraintViolationException ex = assertThrows( ConstraintViolationException.class, tx::commit );
        assertEquals( "Cannot delete node<" + node.getId() + ">, because it still has relationships. " +
                "To delete this node, you must first delete its relationships.", ex.getMessage() );
    }

    @Test
    void shouldDeleteDenseNodeEvenWithTemporarilyCreatedRelationshipsBeforeDeletion()
    {
        // Given
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            nodeId = node.getId();
            for ( int i = 0; i < 200; i++ )
            {
                node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "TYPE_" + i % 3 ) );
            }
            tx.commit();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            // Create temporary relationships of new types, which will be deleted right afterwards
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "OTHER_TYPE_1" ) );
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "OTHER_TYPE_2" ) );
            node.getRelationships().forEach( Relationship::delete );
            node.delete();
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( NotFoundException.class, () -> tx.getNodeById( nodeId ) );
            tx.commit();
        }
    }

    @Test
    void shouldDeleteDenseNodeIfContainingEmptyGroupsFromPreviousContendedRelationshipDeletions() throws ExecutionException, InterruptedException
    {
        // given
        long nodeId;
        RelationshipType typeA = RelationshipType.withName( "A" );
        RelationshipType typeB = RelationshipType.withName( "B" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            nodeId = node.getId();
            for ( int i = 0; i < 200; i++ )
            {
                node.createRelationshipTo( tx.createNode(), i % 2 == 0 ? typeA : typeB );
            }
            tx.commit();
        }

        // when starting a transaction that creates a relationship of type B and halting it before apply
        Barrier.Control barrier;
        try ( OtherThreadExecutor t2 = new OtherThreadExecutor( "T2" ) )
        {
            barrier = new Barrier.Control();
            Future<Object> t2Future = t2.executeDontWait( () ->
            {
                try ( TransactionImpl tx = (TransactionImpl) db.beginTx() )
                {
                    tx.getNodeById( nodeId ).createRelationshipTo( tx.createNode(), typeB );
                    tx.commit( barrier::reached );
                }
                return null;
            } );

            barrier.awaitUninterruptibly();

            // and another transaction which deletes all relationships of type A, and let it commit
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( nodeId ).getRelationships( typeA ).forEach( Relationship::delete );
                tx.commit();
            }
            // and letting the first transaction complete
            barrier.release();
            t2Future.get();
        }

        // then deleting the node should remove the empty group A, even if it's only deleting relationships of type B
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.getRelationships().forEach( relationship ->
            {
                assertThat( relationship.isType( typeB ) ).isTrue();
                relationship.delete();
            } );
            node.delete();
            tx.commit();
        }
    }
}
