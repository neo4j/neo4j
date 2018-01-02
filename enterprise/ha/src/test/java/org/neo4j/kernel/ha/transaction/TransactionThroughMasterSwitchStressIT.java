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
package org.neo4j.kernel.ha.transaction;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.IntFunction;
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
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.UNKNOWN;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.api.KernelTransactions.tx_termination_aware_locks;
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
@RunWith( Parameterized.class )
public class TransactionThroughMasterSwitchStressIT
{
    @Rule
    public final ClusterRule clusterRule;

    public TransactionThroughMasterSwitchStressIT( boolean txTerminationAwareLocks )
    {
        clusterRule = new ClusterRule( getClass() )
                .withSharedSetting( tx_termination_aware_locks, String.valueOf( txTerminationAwareLocks ) )
                .withInstanceSetting( HaSettings.slave_only,
                        new IntFunction<String>() // instances 1 and 2 are slave only
                        {
                            @Override
                            public String apply( int value )
                            {
                                if ( value == 1 || value == 2 )
                                {
                                    return Settings.TRUE;
                                }
                                else
                                {
                                    return Settings.FALSE;
                                }
                            }
                        }
                );
    }

    @Parameters(name = "txTerminationAwareLocks={0}")
    public static List<Object[]> txTerminationAwareLocks()
    {
        return Arrays.asList( new Object[]{false}, new Object[]{true} );
    }

    @Test
    public void shouldNotHaveTransactionsRunningThroughRoleSwitchProduceInconsistencies() throws Throwable
    {
        // Duration of this test. If the timeout is hit in the middle of a round, the round will be completed
        // and exit after that.
        ManagedCluster cluster = clusterRule.startCluster();
        long duration = parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "30s" ) );
        long endTime = currentTimeMillis() + duration;
        while ( currentTimeMillis() < endTime )
        {
            oneRound( cluster );
        }
    }

    private void oneRound( ManagedCluster cluster ) throws Throwable
    {
        // GIVEN a cluster and a node
        final String key = "key";
        final GraphDatabaseService master = cluster.getMaster();
        final long nodeId = createNode( master );
        cluster.sync();

        // and a bunch of workers contending on that node, each changing it
        Workers<Runnable> transactors = new Workers<>( "Transactors" );
        final AtomicInteger successes = new AtomicInteger();
        final AtomicBoolean end = new AtomicBoolean();
        for ( int i = 0; i < 10; i++ )
        {
            transactors.start( new Runnable()
            {
                @Override
                public void run()
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
                            node.setProperty( key, (Integer) node.getProperty( key, 0 )+1 );
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
        transactors.awaitAndThrowOnError( RuntimeException.class );

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
        RepairKit masterRepair = cluster.fail( master, NetworkFlag.IN, NetworkFlag.OUT );
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
