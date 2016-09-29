/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.Exceptions.stringify;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.instanceEvicted;

public class ClusterLocksIT
{
    private static final long TIMEOUT_MILLIS = 120_000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.withSharedSetting( HaSettings.tx_push_factor, "2" ).startCluster();
    }

    private Label testLabel = Label.label( "testLabel" );

    @Test( timeout = TIMEOUT_MILLIS )
    public void lockCleanupOnModeSwitch() throws Throwable
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        createNodeOnMaster( testLabel, master );

        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        ClusterManager.RepairKit repairKit = takeExclusiveLockAndKillSlave( testLabel, slave );

        // repair of slave and new mode switch cycle on all members
        repairKit.repair();
        cluster.await( allSeesAllAsAvailable() );

        HighlyAvailableGraphDatabase clusterMaster = cluster.getMaster();
        // now it should be possible to take exclusive lock on the same node
        takeExclusiveLockOnSameNodeAfterSwitch( testLabel, master, clusterMaster );
    }

    @Test
    public void aPendingMemberShouldBeAbleToServeReads() throws Throwable
    {
        createNodeOnMaster( testLabel, cluster.getMaster() );
        cluster.sync();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        AtomicBoolean run = new AtomicBoolean( true );
        AtomicReference<Throwable> ref = new AtomicReference<>(  );

        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                while ( run.get() )
                {
                    try ( Transaction tx = slave.beginTx() )
                    {
                        Node single = Iterables.single( slave.getAllNodes() );
                        Label label = Iterables.single( single.getLabels() );
                        assertEquals( testLabel, label );
                        tx.success();
                    }
                    catch ( TransactionTerminatedException | TransientTransactionFailureException ex )
                    {
                        // this is fine we might have stopped or rolled back a transaction when we switched role..
                    }
                    catch ( Throwable t )
                    {
                        ref.compareAndSet( null, t );
                    }
                }
            }
        };

        thread.start();

        cluster.fail( slave, ClusterManager.NetworkFlag.values() );
        cluster.await( instanceEvicted( slave ), 20 );

        run.set( false );
        thread.join();

        Throwable throwable = ref.get();
        assertNull( stringify( throwable ), throwable );
    }

    private void takeExclusiveLockOnSameNodeAfterSwitch( Label testLabel, HighlyAvailableGraphDatabase master,
            HighlyAvailableGraphDatabase db ) throws EntityNotFoundException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = getNode( master, testLabel );
            transaction.acquireWriteLock( node );
            node.setProperty( "key", "value" );
            transaction.success();
        }
    }

    private ClusterManager.RepairKit takeExclusiveLockAndKillSlave( Label testLabel, HighlyAvailableGraphDatabase db )
            throws EntityNotFoundException
    {
        Transaction transaction = db.beginTx();
        Node node = getNode( db, testLabel );
        transaction.acquireWriteLock( node );
        return cluster.shutdown( db );
    }

    private void createNodeOnMaster( Label testLabel, HighlyAvailableGraphDatabase master )
    {
        try ( Transaction transaction = master.beginTx() )
        {
            master.createNode( testLabel );
            transaction.success();
        }
    }

    private Node getNode( HighlyAvailableGraphDatabase db, Label testLabel ) throws EntityNotFoundException
    {
        return db.findNodes( testLabel ).stream().findFirst()
                .orElseThrow( () -> new EntityNotFoundException( EntityType.NODE, 0L ) );
    }
}
