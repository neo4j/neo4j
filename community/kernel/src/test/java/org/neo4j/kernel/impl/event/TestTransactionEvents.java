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
package org.neo4j.kernel.impl.event;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.index.DummyIndexExtensionFactory.IDENTIFIER;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class TestTransactionEvents
{
    @Rule
    public final DatabaseRule dbRule = new ImpermanentDatabaseRule();
    private static final TimeUnit AWAIT_INDEX_UNIT = TimeUnit.SECONDS;
    private static final int AWAIT_INDEX_DURATION = 60;

    @Test
    public void testRegisterUnregisterHandlers()
    {
        Object value1 = 10;
        Object value2 = 3.5D;
        DummyTransactionEventHandler<Integer> handler1 = new DummyTransactionEventHandler<>( (Integer) value1 );
        DummyTransactionEventHandler<Double> handler2 = new DummyTransactionEventHandler<>( (Double) value2 );

        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try
        {
            db.unregisterTransactionEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == db.registerTransactionEventHandler( handler1 ) );
        assertTrue( handler1 == db.registerTransactionEventHandler( handler1 ) );
        assertTrue( handler1 == db.unregisterTransactionEventHandler( handler1 ) );

        try
        {
            db.unregisterTransactionEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == db.registerTransactionEventHandler( handler1 ) );
        assertTrue( handler2 == db.registerTransactionEventHandler( handler2 ) );
        assertTrue( handler1 == db.unregisterTransactionEventHandler( handler1 ) );
        assertTrue( handler2 == db.unregisterTransactionEventHandler( handler2 ) );

        db.registerTransactionEventHandler( handler1 );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().delete();
            tx.success();
        }

        assertNotNull( handler1.beforeCommit );
        assertNotNull( handler1.afterCommit );
        assertNull( handler1.afterRollback );
        assertEquals( value1, handler1.receivedState );
        assertNotNull( handler1.receivedTransactionData );
        db.unregisterTransactionEventHandler( handler1 );
    }

    @Test
    public void makeSureHandlersCantBeRegisteredTwice()
    {
        DummyTransactionEventHandler<Object> handler = new DummyTransactionEventHandler<>( null );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        db.registerTransactionEventHandler( handler );
        db.registerTransactionEventHandler( handler );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().delete();
            tx.success();
        }
        assertEquals( Integer.valueOf( 0 ), handler.beforeCommit );
        assertEquals( Integer.valueOf( 1 ), handler.afterCommit );
        assertNull( handler.afterRollback );

        db.unregisterTransactionEventHandler( handler );
    }

    @Test
    public void shouldGetCorrectTransactionDataUponCommit()
    {
        // Create new data, nothing modified, just added/created
        ExpectedTransactionData expectedData = new ExpectedTransactionData();
        VerifyingTransactionEventHandler handler = new VerifyingTransactionEventHandler( expectedData );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        db.registerTransactionEventHandler( handler );
        Node node1;
        Node node2;
        Node node3;
        Relationship rel1;
        Relationship rel2;
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                node1 = db.createNode();
                expectedData.expectedCreatedNodes.add( node1 );

                node2 = db.createNode();
                expectedData.expectedCreatedNodes.add( node2 );

                rel1 = node1.createRelationshipTo( node2, RelTypes.TXEVENT );
                expectedData.expectedCreatedRelationships.add( rel1 );

                node1.setProperty( "name", "Mattias" );
                expectedData.assignedProperty( node1, "name", "Mattias", null );

                node1.setProperty( "last name", "Persson" );
                expectedData.assignedProperty( node1, "last name", "Persson", null );

                node1.setProperty( "counter", 10 );
                expectedData.assignedProperty( node1, "counter", 10, null );

                rel1.setProperty( "description", "A description" );
                expectedData.assignedProperty( rel1, "description",
                        "A description", null );

                rel1.setProperty( "number", 4.5D );
                expectedData.assignedProperty( rel1, "number", 4.5D, null );

                node3 = db.createNode();
                expectedData.expectedCreatedNodes.add( node3 );
                rel2 = node3.createRelationshipTo( node2, RelTypes.TXEVENT );
                expectedData.expectedCreatedRelationships.add( rel2 );

                node3.setProperty( "name", "Node 3" );
                expectedData.assignedProperty( node3, "name", "Node 3", null );
                tx.success();
            }

            assertTrue( "Should have been invoked", handler.hasBeenCalled() );
            Throwable failure = handler.failure();
            if ( failure != null )
            {
                throw new RuntimeException( failure );
            }
        }
        finally
        {
            db.unregisterTransactionEventHandler( handler );
        }

        // Use the above data and modify it, change properties, delete stuff
        expectedData = new ExpectedTransactionData();
        handler = new VerifyingTransactionEventHandler( expectedData );
        db.registerTransactionEventHandler( handler );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node newNode = db.createNode();
                expectedData.expectedCreatedNodes.add( newNode );

                Node tempNode = db.createNode();
                Relationship tempRel = tempNode.createRelationshipTo( node1,
                        RelTypes.TXEVENT );
                tempNode.setProperty( "something", "Some value" );
                tempRel.setProperty( "someproperty", 101010 );
                tempNode.removeProperty( "nothing" );

                node3.setProperty( "test", "hello" );
                node3.setProperty( "name", "No name" );
                node3.delete();
                expectedData.expectedDeletedNodes.add( node3 );
                expectedData.removedProperty( node3, "name", "Node 3" );

                node1.setProperty( "new name", "A name" );
                node1.setProperty( "new name", "A better name" );
                expectedData.assignedProperty( node1, "new name", "A better name",
                        null );
                node1.setProperty( "name", "Nothing" );
                node1.setProperty( "name", "Mattias Persson" );
                expectedData.assignedProperty( node1, "name", "Mattias Persson",
                        "Mattias" );
                node1.removeProperty( "counter" );
                expectedData.removedProperty( node1, "counter", 10 );
                node1.removeProperty( "last name" );
                node1.setProperty( "last name", "Hi" );
                expectedData.assignedProperty( node1, "last name", "Hi", "Persson" );

                rel2.delete();
                expectedData.expectedDeletedRelationships.add( rel2 );

                rel1.removeProperty( "number" );
                expectedData.removedProperty( rel1, "number", 4.5D );
                rel1.setProperty( "description", "Ignored" );
                rel1.setProperty( "description", "New" );
                expectedData.assignedProperty( rel1, "description", "New",
                        "A description" );

                tempRel.delete();
                tempNode.delete();
                tx.success();
            }

            assertTrue( "Should have been invoked", handler.hasBeenCalled() );
            Throwable failure = handler.failure();
            if ( failure != null )
            {
                throw new RuntimeException( failure );
            }
        }
        finally
        {
            db.unregisterTransactionEventHandler( handler );
        }
    }

    @Test
    public void makeSureBeforeAfterAreCalledCorrectly()
    {
        List<TransactionEventHandler<Object>> handlers = new ArrayList<>();
        handlers.add( new FailingEventHandler<>( new DummyTransactionEventHandler<>( null ), false ) );
        handlers.add( new FailingEventHandler<>( new DummyTransactionEventHandler<>( null ), false ) );
        handlers.add( new FailingEventHandler<>( new DummyTransactionEventHandler<>( null ), true ) );
        handlers.add( new FailingEventHandler<>( new DummyTransactionEventHandler<>( null ), false ) );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        for ( TransactionEventHandler<Object> handler : handlers )
        {
            db.registerTransactionEventHandler( handler );
        }

        try
        {
            Transaction tx = db.beginTx();
            try
            {
                db.createNode().delete();
                tx.success();
                tx.close();
                fail( "Should fail commit" );
            }
            catch ( TransactionFailureException e )
            {   // OK
            }
            verifyHandlerCalls( handlers, false );

            db.unregisterTransactionEventHandler( handlers.remove( 2 ) );
            for ( TransactionEventHandler<Object> handler : handlers )
            {
                ((DummyTransactionEventHandler<Object>) ((FailingEventHandler<Object>)handler).source).reset();
            }
            try ( Transaction transaction = db.beginTx() )
            {
                db.createNode().delete();
                transaction.success();
            }
            verifyHandlerCalls( handlers, true );
        }
        finally
        {
            for ( TransactionEventHandler<Object> handler : handlers )
            {
                db.unregisterTransactionEventHandler( handler );
            }
        }
    }

    @Test
    public void shouldBeAbleToAccessExceptionThrownInEventHook()
    {
        class MyFancyException extends Exception
        {

        }

        ExceptionThrowingEventHandler handler = new ExceptionThrowingEventHandler( new MyFancyException(), null, null );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        db.registerTransactionEventHandler( handler );

        try
        {
            Transaction tx = db.beginTx();
            try
            {
                db.createNode().delete();
                tx.success();
                tx.close();
                fail( "Should fail commit" );
            }
            catch ( TransactionFailureException e )
            {
                Throwable currentEx = e;
                do
                {
                    currentEx = currentEx.getCause();
                    if ( currentEx instanceof MyFancyException )
                    {
                        return;
                    }
                }
                while ( currentEx.getCause() != null );
                fail("Expected to find the exception thrown in the event hook as the cause of transaction failure.");
            }
        }
        finally
        {
            db.unregisterTransactionEventHandler( handler );
        }
    }

    @Test
    public void deleteNodeRelTriggerPropertyRemoveEvents()
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Node node1;
        Node node2;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            node2 = db.createNode();
            rel = node1.createRelationshipTo( node2, RelTypes.TXEVENT );
            node1.setProperty( "test1", "stringvalue" );
            node1.setProperty( "test2", 1L );
            rel.setProperty( "test1", "stringvalue" );
            rel.setProperty( "test2", 1L );
            rel.setProperty( "test3", new int[] { 1, 2, 3 } );
            tx.success();
        }
        MyTxEventHandler handler = new MyTxEventHandler();
        db.registerTransactionEventHandler( handler );
        try ( Transaction tx = db.beginTx() )
        {
            rel.delete();
            node1.delete();
            node2.delete();
            tx.success();
        }
        assertEquals( "stringvalue", handler.nodeProps.get( "test1" ) );
        assertEquals( "stringvalue", handler.relProps.get( "test1" ) );
        assertEquals( 1L, handler.nodeProps.get( "test2" ) );
        assertEquals( 1L, handler.relProps.get( "test2" ) );
        int[] intArray = (int[]) handler.relProps.get( "test3" );
        assertEquals( 3, intArray.length );
        assertEquals( 1, intArray[0] );
        assertEquals( 2, intArray[1] );
        assertEquals( 3, intArray[2] );
    }

    private static class MyTxEventHandler implements TransactionEventHandler<Object>
    {
        Map<String,Object> nodeProps = new HashMap<>();
        Map<String,Object> relProps = new HashMap<>();

        @Override
        public void afterCommit( TransactionData data, Object state )
        {
            for ( PropertyEntry<Node> entry : data.removedNodeProperties() )
            {
                String key = entry.key();
                Object value = entry.previouslyCommitedValue();
                nodeProps.put( key, value );
            }
            for ( PropertyEntry<Relationship> entry : data.removedRelationshipProperties() )
            {
                relProps.put( entry.key(), entry.previouslyCommitedValue() );
            }
        }

        @Override
        public void afterRollback( TransactionData data, Object state )
        {
        }

        @Override
        public Object beforeCommit( TransactionData data )
        {
            return null;
        }
    }

    private void verifyHandlerCalls(
            List<TransactionEventHandler<Object>> handlers, boolean txSuccess )
    {
        for ( TransactionEventHandler<Object> handler : handlers )
        {
            DummyTransactionEventHandler<Object> realHandler =
                    (DummyTransactionEventHandler<Object>) ((FailingEventHandler<Object>) handler).source;
            if ( txSuccess )
            {
                assertEquals( Integer.valueOf( 0 ), realHandler.beforeCommit );
                assertEquals( Integer.valueOf( 1 ), realHandler.afterCommit );
            }
            else
            {
                if ( realHandler.counter > 0 )
                {
                    assertEquals( Integer.valueOf( 0 ),
                            realHandler.beforeCommit );
                    assertEquals( Integer.valueOf( 1 ),
                            realHandler.afterRollback );
                }
            }
        }
    }

    private enum RelTypes implements RelationshipType
    {
        TXEVENT
    }

    private static class FailingEventHandler<T> implements TransactionEventHandler<T>
    {
        private final TransactionEventHandler<T> source;
        private final boolean willFail;

        FailingEventHandler( TransactionEventHandler<T> source, boolean willFail )
        {
            this.source = source;
            this.willFail = willFail;
        }

        @Override
        public void afterCommit( TransactionData data, T state )
        {
            source.afterCommit( data, state );
        }

        @Override
        public void afterRollback( TransactionData data, T state )
        {
            source.afterRollback( data, state );
        }

        @Override
        public T beforeCommit( TransactionData data ) throws Exception
        {
            try
            {
                return source.beforeCommit( data );
            }
            finally
            {
                if ( willFail )
                {
                    throw new Exception( "Just failing commit, that's all" );
                }
            }
        }
    }

    private static class ExceptionThrowingEventHandler implements TransactionEventHandler<Object>
    {
        private final Exception beforeCommitException;
        private final Exception afterCommitException;
        private final Exception afterRollbackException;

        ExceptionThrowingEventHandler( Exception exceptionForAll )
        {
            this( exceptionForAll, exceptionForAll, exceptionForAll );
        }

        ExceptionThrowingEventHandler( Exception beforeCommitException, Exception afterCommitException,
                Exception afterRollbackException )
        {
            this.beforeCommitException = beforeCommitException;
            this.afterCommitException = afterCommitException;
            this.afterRollbackException = afterRollbackException;
        }

        @Override
        public Object beforeCommit( TransactionData data ) throws Exception
        {
            if ( beforeCommitException != null )
            {
                throw beforeCommitException;
            }
            return null;
        }

        @Override
        public void afterCommit( TransactionData data, Object state )
        {
            if ( afterCommitException != null )
            {
                throw new RuntimeException( afterCommitException );
            }
        }

        @Override
        public void afterRollback( TransactionData data, Object state )
        {
            if ( afterRollbackException != null )
            {
                throw new RuntimeException( afterRollbackException );
            }
        }
    }

    private static class DummyTransactionEventHandler<T> implements
            TransactionEventHandler<T>
    {
        private final T object;
        private TransactionData receivedTransactionData;
        private T receivedState;
        private int counter;
        private Integer beforeCommit;
        private Integer afterCommit;
        private Integer afterRollback;

        DummyTransactionEventHandler( T object )
        {
            this.object = object;
        }

        @Override
        public void afterCommit( TransactionData data, T state )
        {
            assertNotNull( data );
            this.receivedState = state;
            this.afterCommit = counter++;
        }

        @Override
        public void afterRollback( TransactionData data, T state )
        {
            assertNotNull( data );
            this.receivedState = state;
            this.afterRollback = counter++;
        }

        @Override
        public T beforeCommit( TransactionData data )
        {
            assertNotNull( data );
            this.receivedTransactionData = data;
            this.beforeCommit = counter++;
            if ( this.beforeCommit == 2 )
            {
                new Exception( "blabla" ).printStackTrace();
            }
            return object;
        }

        void reset()
        {
            receivedTransactionData = null;
            receivedState = null;
            counter = 0;
            beforeCommit = null;
            afterCommit = null;
            afterRollback = null;
        }
    }

    @Test
    public void makeSureHandlerIsntCalledWhenTxRolledBack()
    {
        DummyTransactionEventHandler<Integer> handler =
            new DummyTransactionEventHandler<>( 10 );
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        db.registerTransactionEventHandler( handler );
        try
        {
            try ( Transaction ignore = db.beginTx() )
            {
                db.createNode().delete();
            }
            assertNull( handler.beforeCommit );
            assertNull( handler.afterCommit );
            assertNull( handler.afterRollback );
        }
        finally
        {
            db.unregisterTransactionEventHandler( handler );
        }
    }

    @Test
    public void modifiedPropertyCanByFurtherModifiedInBeforeCommit()
    {
        // Given
        // -- create node and set property on it in one transaction
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        final String key = "key";
        final Object value1 = "the old value";
        final Object value2 = "the new value";
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( key, "initial value" );
            tx.success();
        }
        // -- register a tx handler which will override a property
        TransactionEventHandler<Void> handler = new TransactionEventHandler.Adapter<Void>()
        {
            @Override
            public Void beforeCommit( TransactionData data )
            {
                Node modifiedNode = data.assignedNodeProperties().iterator().next().entity();
                assertEquals( node, modifiedNode );
                modifiedNode.setProperty( key, value2 );
                return null;
            }
        };
        db.registerTransactionEventHandler( handler );

        try ( Transaction tx = db.beginTx() )
        {
            // When
            node.setProperty( key, value1 );
            tx.success();
        }
        // Then
        assertThat(node, inTx(db, hasProperty(key).withValue(value2)));
        db.unregisterTransactionEventHandler( handler );
    }

    @Test
    public void nodeCanBecomeSchemaIndexableInBeforeCommitByAddingProperty()
    {
        // Given we have a schema index...
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Label label = label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( "indexed" ).create();
            tx.success();
        }

        // ... and a transaction event handler that likes to add the indexed property on nodes
        db.registerTransactionEventHandler( new TransactionEventHandler.Adapter<Object>()
        {
            @Override
            public Object beforeCommit( TransactionData data )
            {
                Iterator<Node> nodes = data.createdNodes().iterator();
                if ( nodes.hasNext() )
                {
                    Node node = nodes.next();
                    node.setProperty( "indexed", "value" );
                }
                return null;
            }
        } );

        // When we create a node with the right label, but not the right property...
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( AWAIT_INDEX_DURATION, AWAIT_INDEX_UNIT );
            Node node = db.createNode( label );
            node.setProperty( "random", 42 );
            tx.success();
        }

        // Then we should be able to look it up through the index.
        try ( Transaction ignore = db.beginTx() )
        {
            Node node = db.findNode( label, "indexed", "value" );
            assertThat( node.getProperty( "random" ), is( 42 ) );
        }
    }

    @Test
    public void nodeCanBecomeSchemaIndexableInBeforeCommitByAddingLabel()
    {
        // Given we have a schema index...
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        final Label label = label( "Label" );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( "indexed" ).create();
            tx.success();
        }

        // ... and a transaction event handler that likes to add the indexed property on nodes
        db.registerTransactionEventHandler( new TransactionEventHandler.Adapter<Object>()
        {
            @Override
            public Object beforeCommit( TransactionData data )
            {
                Iterator<Node> nodes = data.createdNodes().iterator();
                if ( nodes.hasNext() )
                {
                    Node node = nodes.next();
                    node.addLabel( label );
                }
                return null;
            }
        } );

        // When we create a node with the right property, but not the right label...
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( AWAIT_INDEX_DURATION, AWAIT_INDEX_UNIT );
            Node node = db.createNode();
            node.setProperty( "indexed", "value" );
            node.setProperty( "random", 42 );
            tx.success();
        }

        // Then we should be able to look it up through the index.
        try ( Transaction ignore = db.beginTx() )
        {
            Node node = db.findNode( label, "indexed", "value" );
            assertThat( node.getProperty( "random" ), is( 42 ) );
        }
    }

    @Test
    public void shouldAccessAssignedLabels()
    {
        // given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

        ChangedLabels labels = (ChangedLabels) db.registerTransactionEventHandler( new ChangedLabels() );
        try
        {
            // when
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();

                labels.add( node1, "Foo" );
                labels.add( node2, "Bar" );
                labels.add( node3, "Baz" );
                labels.add( node3, "Bar" );

                labels.activate();
                tx.success();
            }
            // then
            assertTrue( labels.isEmpty() );
        }
        finally
        {
            db.unregisterTransactionEventHandler( labels );
        }
    }

    @Test
    public void shouldAccessRemovedLabels()
    {
        // given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

        ChangedLabels labels = (ChangedLabels) db.registerTransactionEventHandler( new ChangedLabels() );
        try
        {
            Node node1;
            Node node2;
            Node node3;
            try ( Transaction tx = db.beginTx() )
            {
                node1 = db.createNode();
                node2 = db.createNode();
                node3 = db.createNode();

                labels.add( node1, "Foo" );
                labels.add( node2, "Bar" );
                labels.add( node3, "Baz" );
                labels.add( node3, "Bar" );

                tx.success();
            }
            labels.clear();

            // when
            try ( Transaction tx = db.beginTx() )
            {
                labels.remove( node1, "Foo" );
                labels.remove( node2, "Bar" );
                labels.remove( node3, "Baz" );
                labels.remove( node3, "Bar" );

                labels.activate();
                tx.success();
            }
            // then
            assertTrue( labels.isEmpty() );
        }
        finally
        {
            db.unregisterTransactionEventHandler( labels );
        }
    }

    @Test
    public void shouldAccessRelationshipDataInAfterCommit()
    {
        // GIVEN
        final GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        final AtomicInteger accessCount = new AtomicInteger();
        final Map<Long,RelationshipData> expectedRelationshipData = new HashMap<>();
        TransactionEventHandler<Void> handler = new TransactionEventHandler.Adapter<Void>()
        {
            @Override
            public void afterCommit( TransactionData data, Void state )
            {
                accessCount.set( 0 );
                try ( Transaction tx = db.beginTx() )
                {
                    for ( Relationship relationship : data.createdRelationships() )
                    {
                        accessData( relationship );
                    }
                    for ( PropertyEntry<Relationship> change : data.assignedRelationshipProperties() )
                    {
                        accessData( change.entity() );
                    }
                    for ( PropertyEntry<Relationship> change : data.removedRelationshipProperties() )
                    {
                        accessData( change.entity() );
                    }
                    tx.success();
                }
            }

            private void accessData( Relationship relationship )
            {
                accessCount.incrementAndGet();
                RelationshipData expectancy = expectedRelationshipData.get( relationship.getId() );
                assertNotNull( expectancy );
                assertEquals( expectancy.startNode, relationship.getStartNode() );
                assertEquals( expectancy.type, relationship.getType().name() );
                assertEquals( expectancy.endNode, relationship.getEndNode() );
            }
        };
        db.registerTransactionEventHandler( handler );

        // WHEN
        try
        {
            Relationship relationship;
            try ( Transaction tx = db.beginTx() )
            {
                relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
                expectedRelationshipData.put( relationship.getId(), new RelationshipData( relationship ) );
                tx.success();
            }
            // THEN
            assertEquals( 1, accessCount.get() );

            // and WHEN
            try ( Transaction tx = db.beginTx() )
            {
                relationship.setProperty( "name", "Smith" );
                Relationship otherRelationship =
                        db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
                expectedRelationshipData.put( otherRelationship.getId(), new RelationshipData( otherRelationship ) );
                tx.success();
            }
            // THEN
            assertEquals( 2, accessCount.get() );

            // and WHEN
            try ( Transaction tx = db.beginTx() )
            {
                relationship.delete();
                tx.success();
            }
            // THEN
            assertEquals( 1, accessCount.get() );
        }
        finally
        {
            db.unregisterTransactionEventHandler( handler );
        }
    }

    @Test
    public void shouldProvideTheCorrectRelationshipData()
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

        // create a rel type so the next type id is non zero
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), withName( "TYPE" ) );
        }

        RelationshipType livesIn = withName( "LIVES_IN" );
        long relId;

        try ( Transaction tx = db.beginTx() )
        {
            Node person = db.createNode( label( "Person" ) );

            Node city = db.createNode( label( "City" ) );

            Relationship rel = person.createRelationshipTo( city, livesIn );
            rel.setProperty( "since", 2009 );
            relId = rel.getId();
            tx.success();
        }

        final Set<String> changedRelationships = new HashSet<>();

        db.registerTransactionEventHandler( new TransactionEventHandler.Adapter<Void>()
        {
            @Override
            public Void beforeCommit( TransactionData data )
            {
                for ( PropertyEntry<Relationship> entry : data.assignedRelationshipProperties() )
                {
                    changedRelationships.add( entry.entity().getType().name() );
                }

                return null;
            }
        } );

        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = db.getRelationshipById( relId );
            rel.setProperty( "since", 2010 );
            tx.success();
        }

        assertEquals( 1, changedRelationships.size() );
        assertTrue( livesIn + " not in " + changedRelationships.toString(),
                changedRelationships.contains( livesIn.name() ) );
    }

    @Test
    public void shouldNotFireEventForReadOnlyTransaction()
    {
        // GIVEN
        Node root = createTree( 3, 3 );
        dbRule.getGraphDatabaseAPI().registerTransactionEventHandler(
                new ExceptionThrowingEventHandler( new RuntimeException( "Just failing" ) ) );

        // WHEN
        try ( Transaction tx = dbRule.beginTx() )
        {
            count( dbRule.getGraphDatabaseAPI().traversalDescription().traverse( root ) );
            tx.success();
        }
    }

    @Test
    public void shouldNotFireEventForNonDataTransactions()
    {
        // GIVEN
        final AtomicInteger counter = new AtomicInteger();
        dbRule.getGraphDatabaseAPI().registerTransactionEventHandler( new TransactionEventHandler.Adapter<Void>()
        {
            @Override
            public Void beforeCommit( TransactionData data )
            {
                assertTrue( "Expected only transactions that had nodes or relationships created",
                        data.createdNodes().iterator().hasNext() ||
                        data.createdRelationships().iterator().hasNext() );
                counter.incrementAndGet();
                return null;
            }
        } );
        Label label = label( "Label" );
        String key = "key";
        assertEquals( 0, counter.get() );

        // WHEN creating a label token
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.createNode( label );
            tx.success();
        }
        assertEquals( 1, counter.get() );
        // ... a property key token
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.createNode().setProperty( key, "value" );
            tx.success();
        }
        assertEquals( 2, counter.get() );
        // ... and a relationship type
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.createNode().createRelationshipTo( dbRule.createNode(), withName( "A_TYPE" ) );
            tx.success();
        }
        assertEquals( 3, counter.get() );
        // ... also when creating an index
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.schema().indexFor( label ).on( key ).create();
            tx.success();
        }
        // ... or a constraint
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.schema().constraintFor( label ).assertPropertyIsUnique( "otherkey" ).create();
            tx.success();
        }
        // ... or even an explicit index
        try ( Transaction tx = dbRule.beginTx() )
        {
            dbRule.index().forNodes( "some index", stringMap( PROVIDER, IDENTIFIER ) );
            tx.success();
        }

        // THEN only three transaction events (all including graph data) should've been fired
        assertEquals( 3, counter.get() );
    }

    @Test
    public void shouldBeAbleToTouchDataOutsideTxDataInAfterCommit()
    {
        // GIVEN
        final Node node = createNode( "one", "Two", "three", "Four" );
        dbRule.getGraphDatabaseAPI().registerTransactionEventHandler( new TransactionEventHandler.Adapter<Object>()
        {
            @Override
            public void afterCommit( TransactionData data, Object nothing )
            {
                try ( Transaction tx = dbRule.beginTx() )
                {
                    for ( String key : node.getPropertyKeys() )
                    {   // Just to see if one can reach them
                        node.getProperty( key );
                    }
                    tx.success();
                }
            }
        } );

        try ( Transaction tx = dbRule.beginTx() )
        {
            // WHEN/THEN
            dbRule.createNode();
            node.setProperty( "five", "Six" );
            tx.success();
        }
    }

    @Test
    public void shouldAllowToStringOnCreatedRelationshipInAfterCommit()
    {
        // GIVEN
        Relationship relationship;
        Node startNode;
        Node endNode;
        RelationshipType type = MyRelTypes.TEST;
        try ( Transaction tx = dbRule.beginTx() )
        {
            startNode = dbRule.createNode();
            endNode = dbRule.createNode();
            relationship = startNode.createRelationshipTo( endNode, type );
            tx.success();
        }

        // WHEN
        AtomicReference<String> deletedToString = new AtomicReference<>();
        dbRule.registerTransactionEventHandler( new TransactionEventHandler.Adapter<Object>()
        {
            @Override
            public void afterCommit( TransactionData data, Object state )
            {
                for ( Relationship relationship : data.deletedRelationships() )
                {
                    deletedToString.set( relationship.toString() );
                }
            }
        } );
        try ( Transaction tx = dbRule.beginTx() )
        {
            relationship.delete();
            tx.success();
        }

        // THEN
        assertNotNull( deletedToString.get() );
        assertThat( deletedToString.get(), containsString( type.name() ) );
        assertThat( deletedToString.get(), containsString( format( "(%d)", startNode.getId() ) ) );
        assertThat( deletedToString.get(), containsString( format( "(%d)", endNode.getId() ) ) );
    }

    @Test
    public void shouldGetCallToAfterRollbackEvenIfBeforeCommitFailed()
    {
        // given
        CapturingEventHandler<Integer> firstWorkingHandler = new CapturingEventHandler<>( () -> 5 );
        String failureMessage = "Massive fail";
        CapturingEventHandler<Integer> faultyHandler = new CapturingEventHandler<>( () ->
        {
            throw new RuntimeException( failureMessage );
        } );
        CapturingEventHandler<Integer> otherWorkingHandler = new CapturingEventHandler<>( () -> 10 );
        dbRule.registerTransactionEventHandler( firstWorkingHandler );
        dbRule.registerTransactionEventHandler( faultyHandler );
        dbRule.registerTransactionEventHandler( otherWorkingHandler );

        boolean failed = false;
        try ( Transaction tx = dbRule.beginTx() )
        {
            // when
            dbRule.createNode();
            tx.success();
        }
        catch ( Exception e )
        {
            assertTrue( Exceptions.contains( e, failureMessage, RuntimeException.class ) );
            failed = true;
        }
        assertTrue( failed );

        // then
        assertTrue( firstWorkingHandler.beforeCommitCalled );
        assertTrue( firstWorkingHandler.afterRollbackCalled );
        assertEquals( 5, firstWorkingHandler.afterRollbackState.intValue() );
        assertTrue( faultyHandler.beforeCommitCalled );
        assertTrue( faultyHandler.afterRollbackCalled );
        assertNull( faultyHandler.afterRollbackState );
        assertTrue( otherWorkingHandler.beforeCommitCalled );
        assertTrue( otherWorkingHandler.afterRollbackCalled );
        assertEquals( 10, otherWorkingHandler.afterRollbackState.intValue() );
    }

    private Node createNode( String... properties )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.createNode();
            for ( int i = 0; i < properties.length; i++ )
            {
                node.setProperty( properties[i++], properties[i] );
            }
            tx.success();
            return node;
        }
    }

    private Node createTree( int depth, int width )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node root = dbRule.createNode( TestLabels.LABEL_ONE );
            createTree( root, depth, width, 0 );
            tx.success();
            return root;
        }
    }

    private void createTree( Node parent, int maxDepth, int width, int currentDepth )
    {
        if ( currentDepth > maxDepth )
        {
            return;
        }
        for ( int i = 0; i < width; i++ )
        {
            Node child = dbRule.createNode( TestLabels.LABEL_TWO );
            parent.createRelationshipTo( child, MyRelTypes.TEST );
            createTree( child, maxDepth, width, currentDepth + 1 );
        }
    }

    private static final class ChangedLabels extends TransactionEventHandler.Adapter<Void>
    {
        private final Map<Node,Set<String>> added = new HashMap<>();
        private final Map<Node,Set<String>> removed = new HashMap<>();
        private boolean active;

        @Override
        public Void beforeCommit( TransactionData data )
        {
            if ( active )
            {
                check( added, "added to", data.assignedLabels() );
                check( removed, "removed from", data.removedLabels() );
            }
            active = false;
            return null;
        }

        private void check( Map<Node, Set<String>> expected, String change, Iterable<LabelEntry> changes )
        {
            for ( LabelEntry entry : changes )
            {
                Set<String> labels = expected.get(entry.node());
                String message = String.format("':%s' should not be %s %s",
                        entry.label().name(), change, entry.node() );
                assertNotNull( message, labels );
                assertTrue( message, labels.remove( entry.label().name() ) );
                if ( labels.isEmpty() )
                {
                    expected.remove( entry.node() );
                }
            }
            assertTrue(String.format("Expected more labels %s nodes: %s", change, expected), expected.isEmpty());
        }

        public boolean isEmpty()
        {
            return added.isEmpty() && removed.isEmpty();
        }

        public void add( Node node, String label )
        {
            node.addLabel( label( label ) );
            put( added, node, label );
        }

        public void remove( Node node, String label )
        {
            node.removeLabel( label( label ) );
            put( removed, node, label );
        }

        private void put( Map<Node, Set<String>> changes, Node node, String label )
        {
            Set<String> labels = changes.computeIfAbsent( node, k -> new HashSet<>() );
            labels.add( label );
        }

        public void activate()
        {
            assertFalse( isEmpty() );
            active = true;
        }

        public void clear()
        {
            added.clear();
            removed.clear();
            active = false;
        }
    }

    private static class RelationshipData
    {
        final Node startNode;
        final String type;
        final Node endNode;

        RelationshipData( Relationship relationship )
        {
            this.startNode = relationship.getStartNode();
            this.type = relationship.getType().name();
            this.endNode = relationship.getEndNode();
        }
    }

    static class CapturingEventHandler<T> implements TransactionEventHandler<T>
    {
        private final Supplier<T> stateSource;
        boolean beforeCommitCalled;
        boolean afterCommitCalled;
        T afterCommitState;
        boolean afterRollbackCalled;
        T afterRollbackState;

        CapturingEventHandler( Supplier<T> stateSource )
        {
            this.stateSource = stateSource;
        }

        @Override
        public T beforeCommit( TransactionData data )
        {
            beforeCommitCalled = true;
            return stateSource.get();
        }

        @Override
        public void afterCommit( TransactionData data, T state )
        {
            afterCommitCalled = true;
            afterCommitState = state;
        }

        @Override
        public void afterRollback( TransactionData data, T state )
        {
            afterRollbackCalled = true;
            afterRollbackState = state;
        }
    }
}
