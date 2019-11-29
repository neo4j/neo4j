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
package org.neo4j.kernel.impl.core;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.NamedThreadFactory.named;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class NodeEntityTest extends EntityTest
{
    private final String PROPERTY_KEY = "PROPERTY_KEY";

    @Override
    protected long createEntity( Transaction tx )
    {
        return tx.createNode().getId();
    }

    @Override
    protected Entity lookupEntity( Transaction transaction, long id )
    {
        return transaction.getNodeById( id );
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyDoesNotExistOnNode()
    {
        // Given a database with PROPERTY_KEY in it
        createNodeWith( PROPERTY_KEY );

        // When trying to get property from node without it
        NotFoundException exception = assertThrows( NotFoundException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                node.getProperty( PROPERTY_KEY );
            }
        } );
        assertThat( exception.getMessage() ).contains( PROPERTY_KEY );
    }

    @Test
    void createDropNodeLongStringProperty()
    {
        Label markerLabel = Label.label( "marker" );
        String testPropertyKey = "testProperty";
        String propertyValue = RandomStringUtils.randomAscii( 255 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( markerLabel );
            node.setProperty( testPropertyKey, propertyValue );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            assertEquals( propertyValue, node.getProperty( testPropertyKey ) );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            node.removeProperty( testPropertyKey );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            assertFalse( node.hasProperty( testPropertyKey ) );
            tx.commit();
        }
    }

    @Test
    void createDropNodeLongArrayProperty()
    {
        Label markerLabel = Label.label( "marker" );
        String testPropertyKey = "testProperty";
        byte[] propertyValue = RandomUtils.nextBytes( 1024 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( markerLabel );
            node.setProperty( testPropertyKey, propertyValue );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            assertArrayEquals( propertyValue, (byte[]) node.getProperty( testPropertyKey ) );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            node.removeProperty( testPropertyKey );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = Iterators.single( tx.findNodes( markerLabel ) );
            assertFalse( node.hasProperty( testPropertyKey ) );
            tx.commit();
        }
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyDoesNotExist()
    {
        // Given a database without PROPERTY_KEY in it

        // When
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.getProperty( PROPERTY_KEY );
        }
        // Then
        catch ( NotFoundException exception )
        {
            assertThat( exception.getMessage() ).contains( PROPERTY_KEY );
        }
    }

    @Test
    void deletionOfSameNodeTwiceInOneTransactionShouldNotRollbackIt()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            tx.commit();
        }

        // When
        Exception exceptionThrownBySecondDelete = null;

        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            try
            {
                tx.getNodeById( node.getId() ).delete();
            }
            catch ( Exception e )
            {
                exceptionThrownBySecondDelete = e;
            }
            tx.commit();
        }

        // Then
        assertThat( exceptionThrownBySecondDelete ).isInstanceOf( NotFoundException.class );

        assertThrows( NotFoundException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( node.getId() ); // should throw NotFoundException
                tx.commit();
            }
        } );
    }

    @Test
    void deletionOfAlreadyDeletedNodeShouldThrow()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            tx.commit();
        }

        // When
        assertThrows( NotFoundException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( node.getId() ).delete(); // should throw NotFoundException as this node is already deleted
                tx.commit();
            }
        } );
    }

    @Test
    void getAllPropertiesShouldWorkFineWithConcurrentPropertyModifications() throws Exception
    {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool( 2, named( "Test-executor-thread" ) );
        try
        {
            final int propertiesCount = 100;

            final long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                nodeId = node.getId();
                for ( int i = 0; i < propertiesCount; i++ )
                {
                    node.setProperty( "property-" + i, i );
                }
                tx.commit();
            }

            final CountDownLatch start = new CountDownLatch( 1 );
            final AtomicBoolean writerDone = new AtomicBoolean();

            Runnable writer = () ->
            {
                try
                {
                    awaitLatch( start );
                    int propertyKey = 0;
                    while ( propertyKey < propertiesCount )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            Node node = tx.getNodeById( nodeId );
                            for ( int i = 0; i < 10 && propertyKey < propertiesCount; i++, propertyKey++ )
                            {
                                node.setProperty( "property-" + propertyKey, UUID.randomUUID().toString() );
                            }
                            tx.commit();
                        }
                    }
                }
                finally
                {
                    writerDone.set( true );
                }
            };
            Runnable reader = () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = tx.getNodeById( nodeId );
                    awaitLatch( start );
                    while ( !writerDone.get() )
                    {
                        int size = node.getAllProperties().size();
                        assertThat( size ).isGreaterThan( 0 );
                    }
                    tx.commit();
                }
            };

            Future<?> readerFuture = executor.submit( reader );
            Future<?> writerFuture = executor.submit( writer );

            start.countDown();

            // When
            writerFuture.get();
            readerFuture.get();

            // Then
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( propertiesCount, tx.getNodeById( nodeId ).getAllProperties().size() );
                tx.commit();
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    void shouldBeAbleToForceTypeChangeOfProperty()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.setProperty( "prop", 1337 );
            tx.commit();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).setProperty( "prop", 1337.0 );
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( node.getId() ).getProperty( "prop" ) ).isInstanceOf( Double.class );
        }
    }

    @Test
    void shouldOnlyReturnTypeOnce()
    {
        // Given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "R" ) );
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "R" ) );
            node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "R" ) );
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( Iterables.asList( tx.getNodeById( node.getId() ).getRelationshipTypes() ) ).isEqualTo(
                    singletonList( RelationshipType.withName( "R" ) ) );
        }
    }

    @Test
    void shouldThrowCorrectExceptionOnLabelTokensExceeded() throws KernelException
    {
        // given
        var transaction = mockedTransactionWithDepletedTokens();
        NodeEntity nodeEntity = new NodeEntity( transaction, 5 );

        // when
        assertThrows( ConstraintViolationException.class, () -> nodeEntity.addLabel( Label.label( "Label" ) ) );
    }

    @Test
    void shouldThrowCorrectExceptionOnPropertyKeyTokensExceeded() throws KernelException
    {
        // given
        NodeEntity nodeEntity = new NodeEntity( mockedTransactionWithDepletedTokens(), 5 );

        // when
        assertThrows( ConstraintViolationException.class, () -> nodeEntity.setProperty( "key", "value" ) );
    }

    @Test
    void shouldThrowCorrectExceptionOnRelationshipTypeTokensExceeded() throws KernelException
    {
        // given
        NodeEntity nodeEntity = new NodeEntity( mockedTransactionWithDepletedTokens(), 5 );

        // when
        assertThrows( ConstraintViolationException.class, () -> nodeEntity.setProperty( "key", "value" ) );
    }

    private void createNodeWith( String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( key, 1 );
            tx.commit();
        }
    }
}
