/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;

public class TestTransactionEvents
{
    @Test
    public void testRegisterUnregisterHandlers()
    {
        Object value1 = 10;
        Object value2 = 3.5D;
        DummyTransactionEventHandler<Integer> handler1 = new DummyTransactionEventHandler<>( (Integer) value1 );
        DummyTransactionEventHandler<Double> handler2 = new DummyTransactionEventHandler<>( (Double) value2 );

        GraphDatabaseService db = dbRule.getGraphDatabaseService();
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

        assertTrue( handler1 == db.registerTransactionEventHandler(
                handler1 ) );
        assertTrue( handler2 == db.registerTransactionEventHandler(
                handler2 ) );
        assertTrue( handler1 == db.unregisterTransactionEventHandler(
                handler1 ) );
        assertTrue( handler2 == db.unregisterTransactionEventHandler(
                handler2 ) );

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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        db.registerTransactionEventHandler( handler );
        Node node1 = null, node2, node3 = null;
        Relationship rel1 = null, rel2 = null;
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
            if ( handler.failure() != null )
            {
                throw new RuntimeException( handler.failure() );
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
                expectedData.removedProperty( node3, "name", null, "Node 3" );
    
                node1.setProperty( "new name", "A name" );
                node1.setProperty( "new name", "A better name" );
                expectedData.assignedProperty( node1, "new name", "A better name",
                        null );
                node1.setProperty( "name", "Nothing" );
                node1.setProperty( "name", "Mattias Persson" );
                expectedData.assignedProperty( node1, "name", "Mattias Persson",
                        "Mattias" );
                node1.removeProperty( "counter" );
                expectedData.removedProperty( node1, "counter", null, 10 );
                node1.removeProperty( "last name" );
                node1.setProperty( "last name", "Hi" );
                expectedData.assignedProperty( node1, "last name", "Hi", "Persson" );
    
                rel2.delete();
                expectedData.expectedDeletedRelationships.add( rel2 );
    
                rel1.removeProperty( "number" );
                expectedData.removedProperty( rel1, "number", null, 4.5D );
                rel1.setProperty( "description", "Ignored" );
                rel1.setProperty( "description", "New" );
                expectedData.assignedProperty( rel1, "description", "New",
                        "A description" );
    
                tempRel.delete();
                tempNode.delete();
                tx.success();
            }

            assertTrue( "Should have been invoked", handler.hasBeenCalled() );
            if ( handler.failure() != null )
            {
                throw new RuntimeException( handler.failure() );
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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Node node1;
        Node node2;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            node2 = db.createNode();
            rel = node1.createRelationshipTo( node2, RelTypes.TXEVENT );
            node1.setProperty( "test1", "stringvalue" );
            node1.setProperty( "test2", 1l );
            rel.setProperty( "test1", "stringvalue" );
            rel.setProperty( "test2", 1l );
            rel.setProperty( "test3", new int[] { 1, 2, 3 } );
            tx.success();
        }
        MyTxEventHandler handler = new MyTxEventHandler(); 
        db.registerTransactionEventHandler( handler );
        try ( Transaction tx = db.beginTx() )
        {
            GraphDatabaseAPI dbApi = dbRule.getGraphDatabaseAPI();
            dbApi.getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
            rel.delete();
            node1.delete();
            node2.delete();
            dbApi.getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
            tx.success();
        }
        assertEquals( "stringvalue", handler.nodeProps.get( "test1" ) );
        assertEquals( "stringvalue", handler.relProps.get( "test1" ) );
        assertEquals( 1l , handler.nodeProps.get( "test2" ) );
        assertEquals( 1l , handler.relProps.get( "test2" ) );
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
                throws Exception
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

    private static enum RelTypes implements RelationshipType
    {
        TXEVENT
    }

    private static class FailingEventHandler<T> implements TransactionEventHandler<T>
    {
        private final TransactionEventHandler<T> source;
        private final boolean willFail;

        public FailingEventHandler( TransactionEventHandler<T> source, boolean willFail )
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
            if ( willFail )
            {
                throw new Exception( "Just failing commit, that's all" );
            }
            return source.beforeCommit( data );
        }
    }
    
    private static class ExceptionThrowingEventHandler implements TransactionEventHandler<Object>
    {

    	private final Exception beforeCommitException;
    	private final Exception afterCommitException;
    	private final Exception afterRollbackException;
    	
		public ExceptionThrowingEventHandler(Exception beforeCommitException,
				Exception afterCommitException, Exception afterRollbackException) {
			super();
			this.beforeCommitException = beforeCommitException;
			this.afterCommitException = afterCommitException;
			this.afterRollbackException = afterRollbackException;
		}

		@Override
		public Object beforeCommit(TransactionData data) throws Exception 
		{
			if(beforeCommitException != null)
            {
                throw beforeCommitException;
            }
			return null;
		}

		@Override
		public void afterCommit(TransactionData data, Object state) 
		{
			if(afterCommitException != null)
            {
                throw new RuntimeException(afterCommitException);
            }
		}

		@Override
		public void afterRollback(TransactionData data, Object state) 
		{
			if(afterRollbackException != null)
            {
                throw new RuntimeException(afterRollbackException);
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
        private Integer beforeCommit, afterCommit, afterRollback;

        public DummyTransactionEventHandler( T object )
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
		public T beforeCommit( TransactionData data ) throws Exception
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
            new DummyTransactionEventHandler<Integer>( 10 );
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        db.registerTransactionEventHandler( handler );
        try
        {
            try ( Transaction tx = db.beginTx() )
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
    public void modifiedPropertyCanByFurtherModifiedInBeforeCommit() throws Exception
    {
        // Given
        // -- create node and set property on it in one transaction
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
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
            public Void beforeCommit( TransactionData data ) throws Exception
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
    
    public static final @ClassRule DatabaseRule dbRule = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( cache_type, "none" );
        }
    };
}
