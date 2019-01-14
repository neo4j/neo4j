/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.transaction;

import org.junit.Rule;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.NetworkFlag;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.TimeUtil.parseTimeMillis;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.UNKNOWN;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.ha.ClusterManager.memberThinksItIsRole;

/**
 * Non-deterministically tries to reproduce a problem where transactions may, at the time of master switches,
 * sometimes overwrite each others data. More specifically not respect each others locks, among other things.
 * There is no chance this test will yield a false failure, although sometimes it will be successful
 * meaning it didn't manage to reproduce the problem. At the time of writing 2.2.6 and 2.3.0 is out.
 *
 * The master switching in this test focuses on keeping the same master, but fiddle with the cluster so that
 * the master loses quorum and goes to pending, to quickly thereafter go to master role again.
 * Transactions happen before, during and after that (re)election. Each transaction does the following:
 *
 * <ol>
 * <li>Locks the node (all transactions use the same node)
 * <li>Reads an int property from that node
 * <li>Increments and sets that int property value back
 * <li>Commits
 * </ol>
 *
 * Some transactions may not commit, that's fine. For all those that commit the int property (counter)
 * on that node must be incremented by one. In the end, after all threads have.completed, the number of successes
 * is compared with the counter node property. They should be the same. If not, then it means that one or more
 * transactions made changes off of stale values and still managed to commit.
 *
 * This test is a stress test and duration of execution can be controlled via system property
 * -D{@link org.neo4j.kernel.ha.transaction.TransactionThroughMasterSwitchStressIT}.duration
 */
public class TransactionThroughMasterSwitchStressIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withInstanceSetting( HaSettings.slave_only,
                    value -> value == 1 || value == 2 ? Settings.TRUE : Settings.FALSE );

    @Test
    public void shouldNotHaveTransactionsRunningThroughRoleSwitchProduceInconsistencies() throws Throwable
    {
        // Duration of this test. If the timeout is hit in the middle of a round, the round will be completed
        // and exit after that.
        long duration = parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "30s" ) );
        long endTime = currentTimeMillis() + duration;
        while ( currentTimeMillis() < endTime )
        {
            oneRound();
        }
    }

    private void oneRound() throws Throwable
    {
        // GIVEN a cluster and a node
        final String key = "key";
        ManagedCluster cluster = clusterRule.startCluster();
        final GraphDatabaseService master = cluster.getMaster();
        final long nodeId = createNode( master );
        cluster.sync();

        // and a bunch of workers contending on that node, each changing it
        Workers<Runnable> transactors = new Workers<>( "Transactors" );
        final AtomicInteger successes = new AtomicInteger();
        final AtomicBoolean end = new AtomicBoolean();
        for ( int i = 0; i < 10; i++ )
        {
            transactors.start( () ->
            {
                Random random = ThreadLocalRandom.current();
                while ( !end.get() )
                {
                    boolean committed = true;
                    try ( Transaction tx = master.beginTx() )
                    {
                        Node node = master.getNodeById( nodeId );

                        // Acquiring lock, read int property value, increment, set incremented int property
                        // should not break under any circumstances.
                        tx.acquireWriteLock( node );
                        node.setProperty( key, (Integer) node.getProperty( key, 0 ) + 1 );
                        // Throw in relationship for good measure
                        node.createRelationshipTo( master.createNode(), TEST );

                        Thread.sleep( random.nextInt( 1_000 ) );
                        tx.success();
                    }
                    catch ( Throwable e )
                    {
                        // It's OK
                        committed = false;
                    }
                    if ( committed )
                    {
                        successes.incrementAndGet();
                    }
                }
            } );
        }

        // WHEN entering a period of induced cluster instabilities
        reelectTheSameMasterMakingItGoToPendingAndBack( cluster );

        // ... letting transactions run a bit after the role switch as well.
        long targetSuccesses = successes.get() + 20;
        while ( successes.get() < targetSuccesses )
        {
            Thread.sleep( 100 );
        }
        end.set( true );
        transactors.awaitAndThrowOnError();

        // THEN verify that the count is equal to the number of successful transactions
        assertEquals( successes.get(), getNodePropertyValue( master, nodeId, key ) );
    }

    private Object getNodePropertyValue( GraphDatabaseService db, long nodeId, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Object value = db.getNodeById( nodeId ).getProperty( key );
            tx.success();
            return value;
        }
    }

    private void reelectTheSameMasterMakingItGoToPendingAndBack( ManagedCluster cluster ) throws Throwable
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // Fail master and wait for master to go to pending, since it detects it's partitioned away
        RepairKit masterRepair = cluster.fail( master, false, NetworkFlag.IN, NetworkFlag.OUT );
        cluster.await( memberThinksItIsRole( master, UNKNOWN ) );

        // Then Immediately repair
        masterRepair.repair();

        // Wait for this instance to go to master again, since the other instances are slave only
        cluster.await( memberThinksItIsRole( master, MASTER ) );
        cluster.await( ClusterManager.masterAvailable() );
        assertEquals( master, cluster.getMaster() );
    }

    private long createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.success();
            return node.getId();
        }
    }
}
