/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;

public class DeferringLocksIT
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseFacadeFactory.Configuration.deferred_locking, "true" );
        }
    };
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
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
                    node.setProperty( "key", true );
                    tx.success();
                    barrier.reached();
                }
                return null;
            }
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( "key", false );
            tx.success();
            barrier.release();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( node.getPropertyKeys() ) );
            tx.success();
        }
    }

    @Test
    public void firstRemoveSecondChangeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "key", true );
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
                    Object key = node.removeProperty( "key" );
                    tx.success();
                    barrier.reached();
                }
                return null;
            }
        } );
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            node.setProperty( "key", false );
            tx.success();
            barrier.release();
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( (Boolean)node.getProperty( "key", false ) );
            tx.success();
        }
    }

    @Test
    public void removeNodeChangeNodeProperty() throws Exception
    {
        // GIVEN
        final Barrier.Control barrier = new Barrier.Control();
        final long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "key", true );
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
        try ( Transaction tx = db.beginTx() )
        {
            barrier.await();
            db.getNodeById( nodeId ).setProperty( "key", false );
            tx.success();
            barrier.release();
        }

        future.get();
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                db.getNodeById( nodeId );
                assertFalse( (Boolean) db.getNodeById( nodeId ).getProperty( "key", false ) );
            }
            catch ( NotFoundException e )
            {
                // Fine, its gone
            }
            tx.success();
        }
    }

    @Test
    public void nodeAddedToIndexOnCommit() throws Exception
    {
        // GIVEN
        final Label label = DynamicLabel.label( "label" );
        final String key = "key";

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, true );

            t2.execute( createAndAwaitIndex( label, key ) ).get();

            tx.success();
        }

        // THEN
        assertInTxNodeWith( label, key, true );
    }

    @Test
    public void ownChangesAddedToOwnIndex() throws Exception
    {
        // GIVEN
        final Label label = DynamicLabel.label( "label" );
        final String key = "key";

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, true );

            db.schema().indexFor( label ).on( key ).create();

            assertNodeWith( label, key, true );

            tx.success();
        }

        assertInTxNodeWith( label, key, true );
    }

    @Test
    public void readOwnChangesFromRacingIndex() throws Exception
    {
        // GIVEN
        final Label label = DynamicLabel.label( "label" );
        final String key = "key";
        final boolean value = true;

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, value );

            t2.execute( createAndAwaitIndex( label, key ) ).get();

            assertNodeWith( label, key, value );

            tx.success();
        }

        assertInTxNodeWith( label, key, value );
    }

    @Test
    public void readOwnChangesWithoutIndex() throws Exception
    {
        // GIVEN
        final Label label = DynamicLabel.label( "label" );
        final String key = "key";
        final boolean value = true;

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, value );

            assertNodeWith( label, key, value );

            tx.success();
        }

        assertInTxNodeWith( label, key, value );
    }

    void assertInTxNodeWith( Label label, String key, boolean value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNodeWith( label, key, value );
            tx.success();
        }
    }

    void assertNodeWith( Label label, String key, boolean value )
    {
        ResourceIterator<Node> nodes = db.findNodes( label, key, value );
        assertTrue( nodes.hasNext() );
        Node foundNode = nodes.next();
        assertTrue( foundNode.hasLabel( label ) );
        assertTrue( (Boolean) foundNode.getProperty( key ) );
    }

    WorkerCommand<Void,Void> createAndAwaitIndex( final Label label, final String key )
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
                try ( Transaction tx = db.beginTx() ) {
                    db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                }
                return null;
            }
        };
    }
}
