/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.index;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.ha.BeginTx;
import org.neo4j.ha.FinishTx;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.ha.RetryOnGcRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexOperationsIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule(getClass());

    protected ClusterManager.ManagedCluster cluster;

    @Rule
    public RetryOnGcRule retryRule = new RetryOnGcRule();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void index_modifications_are_propagated() throws Exception
    {
        // GIVEN
        // -- a slave
        String key = "name";
        String value = "Mattias";
        HighlyAvailableGraphDatabase author = cluster.getAnySlave();

        // WHEN
        // -- it creates a node associated with indexing in a transaction
        long node = createNode( author, key, value, true );

        // THEN
        // -- all instances should see it after pulling updates
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            db.getDependencyResolver().resolveDependency( UpdatePullerClient.class ).pullUpdates();
            assertNodeAndIndexingExists( db, node, key, value );
        }
    }

    @Test
    public void index_objects_can_be_reused_after_role_switch() throws Exception
    {
        // GIVEN
        // -- an existing index
        String key = "key", value = "value";
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        long nodeId = createNode( master, key, value, true );
        cluster.sync();
        // -- get Index and IndexManager references to all dbs
        Map<HighlyAvailableGraphDatabase,IndexManager> indexManagers = new HashMap<>();
        Map<HighlyAvailableGraphDatabase,Index<Node>> indexes = new HashMap<>();
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            Transaction transaction = db.beginTx();
            try
            {
                indexManagers.put( db, db.index() );
                indexes.put( db, db.index().forNodes( key ) );
                transaction.success();
            }
            finally
            {
                transaction.finish();
            }
        }

        // WHEN
        // -- there's a master switch
        cluster.shutdown( master );
        indexManagers.remove( master );
        indexes.remove( master );

        cluster.await( ClusterManager.masterAvailable( master ) );
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );

        // THEN
        // -- the index instances should still be viable to use
        for ( Map.Entry<HighlyAvailableGraphDatabase, IndexManager> entry : indexManagers.entrySet() )
        {
            HighlyAvailableGraphDatabase db = entry.getKey();
            Transaction transaction = db.beginTx();
            try
            {
                IndexManager indexManager = entry.getValue();
                assertTrue( indexManager.existsForNodes( key ) );
                assertEquals( nodeId, indexManager.forNodes( key ).get( key, value ).getSingle().getId() );
            }
            finally
            {
                transaction.finish();
            }
        }

        for ( Map.Entry<HighlyAvailableGraphDatabase, Index<Node>> entry : indexes.entrySet() )
        {
            HighlyAvailableGraphDatabase db = entry.getKey();
            Transaction transaction = db.beginTx();
            try
            {
                Index<Node> index = entry.getValue();
                assertEquals( nodeId, index.get( key, value ).getSingle().getId() );
            }
            finally
            {
                transaction.finish();
            }
        }
    }

    @Test
    public void put_if_absent_works_across_instances() throws Exception
    {
        // GIVEN
        // -- two instances, each begin a transaction
        String key = "key", value = "value";
        HighlyAvailableGraphDatabase db1 = cluster.getMaster(), db2 = cluster.getAnySlave();
        long node = createNode( db1, key, value, false );
        cluster.sync();
        OtherThreadExecutor<HighlyAvailableGraphDatabase> w1 = new OtherThreadExecutor<>( "w1", db1 );
        OtherThreadExecutor<HighlyAvailableGraphDatabase> w2 = new OtherThreadExecutor<>( "w2", db2 );
        Transaction tx1 = w1.execute( new BeginTx() );
        Transaction tx2 = w2.execute( new BeginTx() );

        // WHEN
        // -- second instance does putIfAbsent --> null
        assertNull( w2.execute( new PutIfAbsent( node, key, value ) ) );
        // -- get a future to first instance putIfAbsent. Wait for it to go and await the lock
        Future<Node> w1Future = w1.executeDontWait( new PutIfAbsent( node, key, value ) );
        w1.waitUntilWaiting();
        // -- second instance completes tx
        w2.execute( new FinishTx( tx2, true ) );
        tx2.success();
        tx2.finish();

        // THEN
        // -- first instance can complete the future with a non-null result
        assertNotNull( w1Future.get() );
        w1.execute( new FinishTx( tx1, true ) );
        // -- assert the index has got one entry and both instances have the same data
        assertNodeAndIndexingExists( db1, node, key, value );
        assertNodeAndIndexingExists( db2, node, key, value );
        cluster.sync();
        assertNodeAndIndexingExists( cluster.getAnySlave( db1, db2 ), node, key, value );

        w2.close();
        w1.close();
    }

    private long createNode( HighlyAvailableGraphDatabase author, String key, Object value, boolean index )
    {
        Transaction tx = author.beginTx();
        try
        {
            Node node = author.createNode();
            node.setProperty( key, value );
            if ( index )
                author.index().forNodes( key ).add( node, key, value );
            tx.success();
            return node.getId();
        }
        catch( Exception e)
        {
            e.printStackTrace( System.err );
            throw e;
        }
        finally
        {
            tx.finish();
        }
    }

    private void assertNodeAndIndexingExists( HighlyAvailableGraphDatabase db, long nodeId, String key, Object value )
    {
        Transaction transaction = db.beginTx();
        try
        {
            Node node = db.getNodeById( nodeId );
            assertEquals( value, node.getProperty( key ) );
            assertTrue( db.index().existsForNodes( key ) );
            assertEquals( node, db.index().forNodes( key ).get( key, value ).getSingle() );
        }
        finally
        {
            transaction.finish();
        }
    }

    private static class PutIfAbsent implements WorkerCommand<HighlyAvailableGraphDatabase, Node>
    {
        private final long node;
        private final String key;
        private final String value;

        public PutIfAbsent( long node, String key, String value )
        {
            this.node = node;
            this.key = key;
            this.value = value;
        }

        @Override
        public Node doWork( HighlyAvailableGraphDatabase state )
        {
            return state.index().forNodes( key ).putIfAbsent( state.getNodeById( node ), key, value );
        }
    }
}
