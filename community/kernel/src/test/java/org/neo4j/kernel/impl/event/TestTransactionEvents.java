/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestTransactionEvents extends AbstractNeo4jTestCase
{
    @Test
    public void testRegisterUnregisterHandlers()
    {
        commit();
        Object value1 = 10;
        Object value2 = 3.5D;
        DummyTransactionEventHandler<Integer> handler1 = new DummyTransactionEventHandler<Integer>(
                (Integer) value1 );
        DummyTransactionEventHandler<Double> handler2 = new DummyTransactionEventHandler<Double>(
                (Double) value2 );

        try
        {
            getGraphDb().unregisterTransactionEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == getGraphDb().registerTransactionEventHandler(
                handler1 ) );
        assertTrue( handler1 == getGraphDb().registerTransactionEventHandler(
                handler1 ) );
        assertTrue( handler1 == getGraphDb().unregisterTransactionEventHandler(
                handler1 ) );

        try
        {
            getGraphDb().unregisterTransactionEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == getGraphDb().registerTransactionEventHandler(
                handler1 ) );
        assertTrue( handler2 == getGraphDb().registerTransactionEventHandler(
                handler2 ) );
        assertTrue( handler1 == getGraphDb().unregisterTransactionEventHandler(
                handler1 ) );
        assertTrue( handler2 == getGraphDb().unregisterTransactionEventHandler(
                handler2 ) );

        getGraphDb().registerTransactionEventHandler( handler1 );
        newTransaction();
        getGraphDb().createNode().delete();
        commit();
        assertNotNull( handler1.beforeCommit );
        assertNotNull( handler1.afterCommit );
        assertNull( handler1.afterRollback );
        assertEquals( value1, handler1.receivedState );
        assertNotNull( handler1.receivedTransactionData );
        getGraphDb().unregisterTransactionEventHandler( handler1 );
    }

    @Test
    public void makeSureHandlersCantBeRegisteredTwice()
    {
        commit();
        DummyTransactionEventHandler<Object> handler =
                new DummyTransactionEventHandler<Object>( null );
        getGraphDb().registerTransactionEventHandler( handler );
        getGraphDb().registerTransactionEventHandler( handler );
        newTransaction();
        getGraphDb().createNode().delete();
        commit();

        assertEquals( Integer.valueOf( 0 ), handler.beforeCommit );
        assertEquals( Integer.valueOf( 1 ), handler.afterCommit );
        assertNull( handler.afterRollback );

        getGraphDb().unregisterTransactionEventHandler( handler );
    }

    @Test
    public void shouldGetCorrectTransactionDataUponCommit()
    {
        makeSureRelationshipTypeIsCreated( RelTypes.TXEVENT );
        
        // Create new data, nothing modified, just added/created
        ExpectedTransactionData expectedData = new ExpectedTransactionData();
        VerifyingTransactionEventHandler handler = new VerifyingTransactionEventHandler(
                expectedData );
        getGraphDb().registerTransactionEventHandler( handler );
        newTransaction();
        Node node1 = null, node2 = null, node3 = null;
        Relationship rel1 = null, rel2 = null;
        try
        {
            node1 = getGraphDb().createNode();
            expectedData.expectedCreatedNodes.add( node1 );

            node2 = getGraphDb().createNode();
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

            node3 = getGraphDb().createNode();
            expectedData.expectedCreatedNodes.add( node3 );
            rel2 = node3.createRelationshipTo( node2, RelTypes.TXEVENT );
            expectedData.expectedCreatedRelationships.add( rel2 );

            node3.setProperty( "name", "Node 3" );
            expectedData.assignedProperty( node3, "name", "Node 3", null );

            newTransaction();
            assertTrue( handler.hasBeenCalled() );
        }
        finally
        {
            getGraphDb().unregisterTransactionEventHandler( handler );
        }

        // Use the above data and modify it, change properties, delete stuff
        expectedData = new ExpectedTransactionData();
        handler = new VerifyingTransactionEventHandler( expectedData );
        getGraphDb().registerTransactionEventHandler( handler );
        newTransaction();
        try
        {
            Node tempNode = getGraphDb().createNode();
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

            newTransaction();
        }
        finally
        {
            getGraphDb().unregisterTransactionEventHandler( handler );
        }
    }

    private void makeSureRelationshipTypeIsCreated( RelationshipType type )
    {
        Node dummy1 = getGraphDb().createNode();
        Node dummy2 = getGraphDb().createNode();
        dummy1.createRelationshipTo( dummy2, type ).delete();
        dummy1.delete();
        dummy2.delete();
    }

    @Test
    public void makeSureBeforeAfterAreCalledCorrectly()
    {
        commit();

        List<TransactionEventHandler<Object>> handlers =
                new ArrayList<TransactionEventHandler<Object>>();
        handlers.add( new FailingEventHandler<Object>(
                new DummyTransactionEventHandler<Object>( null ), false ) );
        handlers.add( new FailingEventHandler<Object>(
                new DummyTransactionEventHandler<Object>( null ), false ) );
        handlers.add( new FailingEventHandler<Object>(
                new DummyTransactionEventHandler<Object>( null ), true ) );
        handlers.add( new FailingEventHandler<Object>(
                new DummyTransactionEventHandler<Object>( null ), false ) );
        for ( TransactionEventHandler<Object> handler : handlers )
        {
            getGraphDb().registerTransactionEventHandler( handler );
        }

        try
        {
            newTransaction();
            getGraphDb().createNode().delete();
            try
            {
                commit();
                fail( "Should fail commit" );
            }
            catch ( TransactionFailureException e )
            {
                // OK
            }
            verifyHandlerCalls( handlers, false );

            getGraphDb().unregisterTransactionEventHandler( handlers.remove( 2 ) );
            for ( TransactionEventHandler<Object> handler : handlers )
            {
                ((DummyTransactionEventHandler<Object>) ((FailingEventHandler<Object>)handler).source).reset();
            }
            newTransaction();
            getGraphDb().createNode().delete();
            commit();
            verifyHandlerCalls( handlers, true );
        }
        finally
        {
            for ( TransactionEventHandler<Object> handler : handlers )
            {
                getGraphDb().unregisterTransactionEventHandler( handler );
            }
        }
    }
    
    @Test
    public void shouldBeAbleToAccessExceptionThrownInEventHook()
    {
        commit();

        class MyFancyException extends Exception 
        {
        	
        }
        
        List<TransactionEventHandler<Object>> handlers =
                new ArrayList<TransactionEventHandler<Object>>();
        handlers.add( new ExceptionThrowingEventHandler(new MyFancyException(), null, null) );
        
        for ( TransactionEventHandler<Object> handler : handlers )
        {
            getGraphDb().registerTransactionEventHandler( handler );
        }

        try
        {
            newTransaction();
            getGraphDb().createNode().delete();
            try
            {
                commit();
                fail( "Should fail commit" );
            }
            catch ( TransactionFailureException e )
            {
            	Throwable currentEx = e;
            	do {
            		currentEx = currentEx.getCause();
                	if(currentEx instanceof MyFancyException)
                	{
                		return;
                	}
                } while(currentEx.getCause() != null);
                fail("Expected to find the exception thrown in the event hook as the cause of transaction failure.");
            }
        }
        finally
        {
            for ( TransactionEventHandler<Object> handler : handlers )
            {
                getGraphDb().unregisterTransactionEventHandler( handler );
            }
        }
    }
    
    @Test
    public void deleteNodeRelTriggerPropertyRemoveEvents()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, RelTypes.TXEVENT );
        node1.setProperty( "test1", "stringvalue" );
        node1.setProperty( "test2", 1l );
        rel.setProperty( "test1", "stringvalue" );
        rel.setProperty( "test2", 1l );
        rel.setProperty( "test3", new int[] { 1,2,3 } );
        commit();
        MyTxEventHandler handler = new MyTxEventHandler(); 
        getGraphDb().registerTransactionEventHandler( handler );
        newTransaction();
        getEmbeddedGraphDb().getNodeManager().clearCache();
        rel.delete();
        node1.delete();
        node2.delete();
        getEmbeddedGraphDb().getNodeManager().clearCache();
        commit();
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
        Map<String,Object> nodeProps = new HashMap<String,Object>();
        Map<String,Object> relProps = new HashMap<String, Object>();
        
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
            // TODO Auto-generated method stub
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
				throw beforeCommitException;
			return null;
		}

		@Override
		public void afterCommit(TransactionData data, Object state) 
		{
			if(afterCommitException != null)
				throw new RuntimeException(afterCommitException);
		}

		@Override
		public void afterRollback(TransactionData data, Object state) 
		{
			if(afterRollbackException != null)
				throw new RuntimeException(afterRollbackException);
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
        commit();
        DummyTransactionEventHandler<Integer> handler =
            new DummyTransactionEventHandler<Integer>( 10 );
        getGraphDb().registerTransactionEventHandler( handler );
        try
        {
            newTransaction();
            getGraphDb().createNode().delete();
            rollback();
            assertNull( handler.beforeCommit );
            assertNull( handler.afterCommit );
            assertNull( handler.afterRollback );
        }
        finally
        {
            getGraphDb().unregisterTransactionEventHandler( handler );
        }
    }
}