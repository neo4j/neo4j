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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;

/**
 * Test for randomly creating data and verifying transaction data seen in transaction event handlers.
 */
@ExtendWith( RandomExtension.class )
@ImpermanentDbmsExtension
class TransactionEventsIT
{
    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private RandomRule random;

    @Test
    void shouldSeeExpectedTransactionData()
    {
        // GIVEN
        final Graph state = new Graph( db, random );
        final ExpectedTransactionData expected = new ExpectedTransactionData( true );
        final TransactionEventListener<Object> listener = new VerifyingTransactionEventListener( expected );
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Operation.createNode.perform( state, expected );
            }
            for ( int i = 0; i < 20; i++ )
            {
                Operation.createRelationship.perform( state, expected );
            }
            tx.commit();
        }

        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, listener );

        // WHEN
        Operation[] operations = Operation.values();
        for ( int i = 0; i < 1_000; i++ )
        {
            expected.clear();
            try ( Transaction tx = db.beginTx() )
            {
                int transactionSize = random.intBetween( 1, 20 );
                for ( int j = 0; j < transactionSize; j++ )
                {
                    random.among( operations ).perform( state, expected );
                }
                tx.commit();
            }
        }

        // THEN the verifications all happen inside the transaction event listener
    }

    @Test
    void transactionIdAndCommitTimeAccessibleAfterCommit()
    {
        TransactionIdCommitTimeTracker commitTimeTracker = new TransactionIdCommitTimeTracker();
        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, commitTimeTracker );

        runTransaction();

        long firstTransactionId = commitTimeTracker.getTransactionIdAfterCommit();
        long firstTransactionCommitTime = commitTimeTracker.getCommitTimeAfterCommit();
        assertTrue( firstTransactionId > 0, "Should be positive tx id." );
        assertTrue( firstTransactionCommitTime > 0, "Should be positive." );

        runTransaction();

        long secondTransactionId = commitTimeTracker.getTransactionIdAfterCommit();
        long secondTransactionCommitTime = commitTimeTracker.getCommitTimeAfterCommit();
        assertTrue( secondTransactionId > 0, "Should be positive tx id." );
        assertTrue( secondTransactionCommitTime > 0, "Should be positive commit time value." );

        assertTrue( secondTransactionId > firstTransactionId, "Second tx id should be higher then first one." );
        assertTrue( secondTransactionCommitTime >= firstTransactionCommitTime, "Second commit time should be higher or equals then first one." );
    }

    @Test
    void transactionIdNotAccessibleBeforeCommit()
    {
        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, getBeforeCommitListener( TransactionData::getTransactionId ) );
        Throwable rootCause = Exceptions.rootCause( assertThrows( RuntimeException.class, this::runTransaction ) );
        assertTrue( rootCause instanceof IllegalStateException );
        assertEquals( "Transaction id is not assigned yet. It will be assigned during transaction commit.", rootCause.getMessage() );
    }

    @Test
    void commitTimeNotAccessibleBeforeCommit()
    {
        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, getBeforeCommitListener( TransactionData::getCommitTime ) );
        Throwable rootCause = Exceptions.rootCause( assertThrows( RuntimeException.class, this::runTransaction ) );
        assertTrue( rootCause instanceof IllegalStateException );
        assertEquals( "Transaction commit time is not assigned yet. It will be assigned during transaction commit.", rootCause.getMessage() );
    }

    @Test
    void shouldGetEmptyUsernameOnAuthDisabled()
    {
        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, getBeforeCommitListener( txData ->
        {
            assertThat( "Should have no username", txData.username(), equalTo( "" ) );
            assertThat( "Should have no metadata", txData.metaData(), equalTo( Collections.emptyMap() ) );
        }) );
        runTransaction();
    }

    @Test
    void shouldGetSpecifiedUsernameAndMetaDataInTXData()
    {
        final AtomicReference<String> usernameRef = new AtomicReference<>();
        final AtomicReference<Map<String,Object>> metaDataRef = new AtomicReference<>();
        dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, getBeforeCommitListener( txData ->
        {
            usernameRef.set( txData.username() );
            metaDataRef.set( txData.metaData() );
        } ) );
        AuthSubject subject = mock( AuthSubject.class );
        when( subject.username() ).thenReturn( "Christof" );
        LoginContext loginContext = new LoginContext()
        {
            @Override
            public AuthSubject subject()
            {
                return subject;
            }

            @Override
            public SecurityContext authorize( IdLookup idLookup, String dbName )
            {
                return new SecurityContext( subject, AccessMode.Static.WRITE );
            }
        };
        Map<String,Object> metadata = genericMap( "username", "joe" );
        runTransaction( loginContext, metadata );

        assertThat( "Should have specified username", usernameRef.get(), equalTo( "Christof" ) );
        assertThat( "Should have metadata with specified username", metaDataRef.get(), equalTo( metadata ) );
    }

    @Test
    void registerUnregisterWithConcurrentTransactions() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        try
        {
            AtomicInteger runningCounter = new AtomicInteger();
            AtomicInteger doneCounter = new AtomicInteger();
            BinaryLatch startLatch = new BinaryLatch();
            RelationshipType relationshipType = RelationshipType.withName( "REL" );
            CountingTransactionEventListener[] handlers = new CountingTransactionEventListener[20];
            for ( int i = 0; i < handlers.length; i++ )
            {
                handlers[i] = new CountingTransactionEventListener();
            }
            long relNodeId;
            try ( Transaction tx = db.beginTx() )
            {
                relNodeId = db.createNode().getId();
                tx.commit();
            }
            Future<?> nodeCreator = executor.submit( () ->
            {
                try
                {
                    runningCounter.incrementAndGet();
                    startLatch.await();
                    for ( int i = 0; i < 2_000; i++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            db.createNode();
                            if ( ThreadLocalRandom.current().nextBoolean() )
                            {
                                tx.commit();
                            }
                        }
                    }
                }
                finally
                {
                    doneCounter.incrementAndGet();
                }
            } );
            Future<?> relationshipCreator = executor.submit( () ->
            {
                try
                {
                    runningCounter.incrementAndGet();
                    startLatch.await();
                    for ( int i = 0; i < 1_000; i++ )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            Node relNode = db.getNodeById( relNodeId );
                            relNode.createRelationshipTo( relNode, relationshipType );
                            if ( ThreadLocalRandom.current().nextBoolean() )
                            {
                                tx.commit();
                            }
                        }
                    }
                }
                finally
                {
                    doneCounter.incrementAndGet();
                }
            } );
            while ( runningCounter.get() < 2 )
            {
                Thread.yield();
            }
            int i = 0;
            dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, handlers[i] );
            CountingTransactionEventListener currentlyRegistered = handlers[i];
            i++;
            startLatch.release();
            while ( doneCounter.get() < 2 )
            {
                dbms.registerTransactionEventListener( DEFAULT_DATABASE_NAME, handlers[i] );
                i++;
                if ( i == handlers.length )
                {
                    i = 0;
                }
                dbms.unregisterTransactionEventListener( DEFAULT_DATABASE_NAME, currentlyRegistered );
                currentlyRegistered = handlers[i];
            }
            nodeCreator.get();
            relationshipCreator.get();
            for ( CountingTransactionEventListener handler : handlers )
            {
                assertEquals( 0, handler.get() );
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static TransactionEventListenerAdapter<Object> getBeforeCommitListener( Consumer<TransactionData> dataConsumer )
    {
        return new TransactionEventListenerAdapter<>()
        {
            @Override
            public Object beforeCommit( TransactionData data, GraphDatabaseService databaseService ) throws Exception
            {
                dataConsumer.accept( data );
                return super.beforeCommit( data, databaseService );
            }
        };
    }

    private void runTransaction()
    {
        runTransaction( AnonymousContext.write(), Collections.emptyMap() );
    }

    private void runTransaction( LoginContext loginContext, Map<String,Object> metaData )
    {
        try ( Transaction transaction = db.beginTransaction( KernelTransaction.Type.explicit, loginContext ) )
        {
            KernelTransaction kernelTransaction =
                    db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                            .getKernelTransactionBoundToThisThread( true, db.databaseId() );
            kernelTransaction.setMetaData( metaData );
            db.createNode();
            transaction.commit();
        }
    }

    enum Operation
    {
        createNode
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.createNode();
                expectations.createdNode( node );
                debug( node );
            }
        },
        deleteNode
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    for ( Relationship relationship : node.getRelationships() )
                    {
                        graph.deleteRelationship( relationship );
                        expectations.deletedRelationship( relationship );
                        debug( relationship );
                    }
                    graph.deleteNode( node );
                    expectations.deletedNode( node );
                    debug( node );
                }
            }
        },
        assignLabel
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    Label label = graph.randomLabel();
                    if ( !node.hasLabel( label ) )
                    {
                        node.addLabel( label );
                        expectations.assignedLabel( node, label );
                        debug( node + " " + label );
                    }
                }
            }
        },
        removeLabel
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    Label label = graph.randomLabel();
                    if ( node.hasLabel( label ) )
                    {
                        node.removeLabel( label );
                        expectations.removedLabel( node, label );
                        debug( node + " " + label );
                    }
                }
            }
        },
        setNodeProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    String key = graph.randomPropertyKey();
                    Object valueBefore = node.getProperty( key, null );
                    Object value = graph.randomPropertyValue();
                    node.setProperty( key, value );
                    expectations.assignedProperty( node, key, value, valueBefore );
                    debug( node + " " + key + "=" + value + " prev " + valueBefore );
                }
            }
        },
        removeNodeProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Node node = graph.randomNode();
                if ( node != null )
                {
                    String key = graph.randomPropertyKey();
                    if ( node.hasProperty( key ) )
                    {
                        Object valueBefore = node.removeProperty( key );
                        expectations.removedProperty( node, key, valueBefore );
                        debug( node + " " + key + "=" + valueBefore );
                    }
                }
            }
        },
        setRelationshipProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    String key = graph.randomPropertyKey();
                    Object valueBefore = relationship.getProperty( key, null );
                    Object value = graph.randomPropertyValue();
                    relationship.setProperty( key, value );
                    expectations.assignedProperty( relationship, key, value, valueBefore );
                    debug( relationship + " " + key + "=" + value + " prev " + valueBefore );
                }
            }
        },
        removeRelationshipProperty
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    String key = graph.randomPropertyKey();
                    if ( relationship.hasProperty( key ) )
                    {
                        Object valueBefore = relationship.removeProperty( key );
                        expectations.removedProperty( relationship, key, valueBefore );
                        debug( relationship + " " + key + "=" + valueBefore );
                    }
                }
            }
        },
        createRelationship
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                while ( graph.nodeCount() < 2 )
                {
                    createNode.perform( graph, expectations );
                }
                Node node1 = graph.randomNode();
                Node node2 = graph.randomNode();
                Relationship relationship = graph.createRelationship( node1, node2, graph.randomRelationshipType() );
                expectations.createdRelationship( relationship );
                debug( relationship );
            }
        },
        deleteRelationship
        {
            @Override
            void perform( Graph graph, ExpectedTransactionData expectations )
            {
                Relationship relationship = graph.randomRelationship();
                if ( relationship != null )
                {
                    graph.deleteRelationship( relationship );
                    expectations.deletedRelationship( relationship );
                    debug( relationship );
                }
            }
        };

        abstract void perform( Graph graph, ExpectedTransactionData expectations );

        void debug( Object value )
        {   // Add a system.out here if you need to debug this case a bit easier
        }
    }

    private static class Graph
    {
        private static final String[] TOKENS = {"A", "B", "C", "D", "E"};

        private final GraphDatabaseService db;
        private final RandomRule random;
        private final List<Node> nodes = new ArrayList<>();
        private final List<Relationship> relationships = new ArrayList<>();

        Graph( GraphDatabaseService db, RandomRule random )
        {
            this.db = db;
            this.random = random;
        }

        private <E extends PropertyContainer> E random( List<E> entities )
        {
            return entities.isEmpty() ? null : entities.get( random.nextInt( entities.size() ) );
        }

        Node randomNode()
        {
            return random( nodes );
        }

        Relationship randomRelationship()
        {
            return random( relationships );
        }

        Node createNode()
        {
            Node node = db.createNode();
            nodes.add( node );
            return node;
        }

        void deleteRelationship( Relationship relationship )
        {
            relationship.delete();
            relationships.remove( relationship );
        }

        void deleteNode( Node node )
        {
            node.delete();
            nodes.remove( node );
        }

        private String randomToken()
        {
            return random.among( TOKENS );
        }

        Label randomLabel()
        {
            return Label.label( randomToken() );
        }

        RelationshipType randomRelationshipType()
        {
            return RelationshipType.withName( randomToken() );
        }

        String randomPropertyKey()
        {
            return randomToken();
        }

        Object randomPropertyValue()
        {
            return random.nextValueAsObject();
        }

        int nodeCount()
        {
            return nodes.size();
        }

        Relationship createRelationship( Node node1, Node node2, RelationshipType type )
        {
            Relationship relationship = node1.createRelationshipTo( node2, type );
            relationships.add( relationship );
            return relationship;
        }
    }

    private static class TransactionIdCommitTimeTracker extends TransactionEventListenerAdapter<Object>
    {

        private long transactionIdAfterCommit;
        private long commitTimeAfterCommit;

        @Override
        public Object beforeCommit( TransactionData data, GraphDatabaseService databaseService ) throws Exception
        {
            return super.beforeCommit( data, databaseService );
        }

        @Override
        public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            commitTimeAfterCommit = data.getCommitTime();
            transactionIdAfterCommit = data.getTransactionId();
            super.afterCommit( data, state, databaseService );
        }

        long getTransactionIdAfterCommit()
        {
            return transactionIdAfterCommit;
        }

        long getCommitTimeAfterCommit()
        {
            return commitTimeAfterCommit;
        }
    }

    private static class CountingTransactionEventListener
            extends AtomicInteger
            implements TransactionEventListener<CountingTransactionEventListener>
    {

        @Override
        public CountingTransactionEventListener beforeCommit( TransactionData data, GraphDatabaseService databaseService )
        {
            getAndIncrement();
            return this;
        }

        @Override
        public void afterCommit( TransactionData data, CountingTransactionEventListener state, GraphDatabaseService databaseService )
        {
            getAndDecrement();
            assertThat( state, sameInstance( this ) );
        }

        @Override
        public void afterRollback( TransactionData data, CountingTransactionEventListener state, GraphDatabaseService databaseService )
        {
            getAndDecrement();
            assertThat( state, sameInstance( this ) );
        }
    }
}
