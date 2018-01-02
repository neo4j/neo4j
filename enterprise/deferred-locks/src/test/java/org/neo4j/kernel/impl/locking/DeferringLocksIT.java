/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.EnterpriseDatabaseRule;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;

public class DeferringLocksIT
{
    private static final long TEST_TIMEOUT = 30_000;

    private static final Label LABEL = DynamicLabel.label( "label" );
    private static final String PROPERTY_KEY = "key";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";

    @Rule
    public final DatabaseRule dbRule = new EnterpriseDatabaseRule().startLazily();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();
    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>();

    private GraphDatabaseService db;

    @Before
    public void initDb() throws Exception
    {
        dbRule.setConfig( DeferringStatementLocksFactory.deferred_locks_enabled, Settings.TRUE );
        db = dbRule.getGraphDatabaseAPI();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void shouldNotFreakOutIfTwoTransactionsDecideToEachAddTheSameProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }

        // WHEN
        t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.setProperty( PROPERTY_KEY, VALUE_1 );
                    tx.success();
                    barrier.reached();
                }
                return null;
            }
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( PROPERTY_KEY, VALUE_2 );
            tx.success();
            barrier.release();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( node.getPropertyKeys() ) );
            tx.success();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void firstRemoveSecondChangeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( PROPERTY_KEY, VALUE_1 );
            tx.success();
        }

        // WHEN
        Future<Void> future = t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    node.removeProperty( PROPERTY_KEY );
                    tx.success();
                    barrier.reached();
                }
                return null;
            }
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( PROPERTY_KEY, VALUE_2 );
            tx.success();
            barrier.release();
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( VALUE_2, node.getProperty( PROPERTY_KEY, VALUE_2 ) );
            tx.success();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void removeNodeChangeNodeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( PROPERTY_KEY, VALUE_1 );
            tx.success();
        }

        // WHEN
        Future<Void> future = t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.getNodeById( nodeId ).delete();
                    tx.success();
                    barrier.reached();
                }
                return null;
            }
        } );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                barrier.await();
                db.getNodeById( nodeId ).setProperty( PROPERTY_KEY, VALUE_2 );
                tx.success();
                barrier.release();
            }
        }
        catch ( TransactionFailureException e )
        {
            // Node was already deleted, fine.
            assertThat( e.getCause(), instanceOf( InvalidRecordException.class ) );
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                db.getNodeById( nodeId );
                assertEquals( VALUE_2, db.getNodeById( nodeId ).getProperty( PROPERTY_KEY, VALUE_2 ) );
            }
            catch ( NotFoundException e )
            {
                // Fine, its gone
            }
            tx.success();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void readOwnChangesFromRacingIndexNoBlock() throws Throwable
    {
        Future<Void> t2Future = t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createNodeWithProperty( LABEL, PROPERTY_KEY, VALUE_1 );
                    assertNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );

                    tx.success();
                }
                return null;
            }
        } );

        Future<Void> t3Future = t3.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createAndAwaitIndex( LABEL, PROPERTY_KEY );
                    tx.success();
                }
                return null;
            }
        } );

        t3Future.get();
        t2Future.get();

        assertInTxNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void readOwnChangesWithoutIndex() throws Exception
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            node.setProperty( PROPERTY_KEY, VALUE_1 );

            assertNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );

            tx.success();
        }

        assertInTxNodeWith( LABEL, PROPERTY_KEY, VALUE_1 );
    }

    private void assertInTxNodeWith( Label label, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNodeWith( label, key, value );
            tx.success();
        }
    }

    private void assertNodeWith( Label label, String key, Object value )
    {
        ResourceIterator<Node> nodes = db.findNodes( label, key, value );
        assertTrue( nodes.hasNext() );
        Node foundNode = nodes.next();
        assertTrue( foundNode.hasLabel( label ) );
        assertEquals( value, foundNode.getProperty( key ) );
    }

    private Node createNodeWithProperty( Label label, String key, Object value )
    {
        Node node = db.createNode( label );
        node.setProperty( key, value );
        return node;
    }

    private WorkerCommand<Void,Void> createNode( final Label label, final String propertyKey,
            final Object propertyValue )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( propertyKey, propertyValue );
                    tx.success();
                }
                return null;
            }
        };
    }

    private WorkerCommand<Void,Void> createIndexOn( final Label label, final String propertyKey )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().indexFor( label ).on( propertyKey ).create();
                    tx.success();
                }
                return null;
            }
        };
    }

    private WorkerCommand<Void,Void> createUniquenessConstraintOn( final Label label, final String propertyKey )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
                    tx.success();
                }
                return null;
            }
        };
    }

    private WorkerCommand<Void,Void> createAndAwaitIndex( final Label label, final String key )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().indexFor( label ).on( key ).create();
                    tx.success();
                }
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                }
                return null;
            }
        };
    }

    private static <T> T get( Future<T> future ) throws InterruptedException, ExecutionException, TimeoutException
    {
        return future.get( 20, TimeUnit.SECONDS );
    }
}
