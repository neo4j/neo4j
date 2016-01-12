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
package org.neo4j.ha;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;

/**
 * This is a test for the Neo4j HA self-inflicted DDOS "pull storm" phenomenon. In a 2 instance setup, whereby
 * the slave has been down for awhile thus causing it to be substantially behind on transactions, when it comes back online
 * and the master tries to push to it, if many transactions concurrently commit, causing concurrent pullUpdate calls,
 * this may cause a DDOS on itself, due to too many concurrent transaction synchronizations, causing timeouts.
 *
 */
@Ignore("A good idea but the test is too high level, is fragile and takes too long.")
public class PullStormIT
{
    @ClassRule
    public static ClusterRule clusterRule = new ClusterRule( PullStormIT.class )
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withSharedSetting( HaSettings.tx_push_factor, "1" );

    @ClassRule
    public static LoggerRule logger = new LoggerRule( Level.OFF );

    @Test
    public void testPullStorm() throws Throwable
    {
        // given
        ManagedCluster cluster = clusterRule.startCluster();

        // Create data
        final HighlyAvailableGraphDatabase master = cluster.getMaster();
        {
            try ( Transaction tx = master.beginTx() )
            {
                for ( int i = 0; i < 1000; i++ )
                {
                    master.createNode().setProperty( "foo", "bar" );
                }
                tx.success();
            }
        }

        // Slave goes down
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        ClusterManager.RepairKit repairKit = cluster.fail( slave );

        // Create more data
        for ( int i = 0; i < 1000; i++ )
        {
            {
                try (Transaction tx = master.beginTx())
                {
                    for ( int j = 0; j < 1000; j++ )
                    {
                        master.createNode().setProperty( "foo", "bar" );
                        master.createNode().setProperty( "foo", "bar" );
                    }
                    tx.success();
                }
            }
        }

        // Slave comes back online
        repairKit.repair();

        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );

        // when

        // Create 20 concurrent transactions
        ExecutorService executor = Executors.newFixedThreadPool( 20 );
        for ( int i = 0; i < 20; i++ )
        {
            executor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    // Transaction close should cause lots of concurrent calls to pullUpdate()
                    try ( Transaction tx = master.beginTx() )
                    {
                        master.createNode().setProperty( "foo", "bar" );
                        tx.success();
                    }
                }
            } );
        }

        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.MINUTES );

        // then
        long masterLastCommittedTxId = lastCommittedTxId( master );
        for ( HighlyAvailableGraphDatabase member : cluster.getAllMembers() )
        {
            assertEquals( masterLastCommittedTxId, lastCommittedTxId( member ) );
        }
    }

    private long lastCommittedTxId( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase )
    {
        return highlyAvailableGraphDatabase.getDependencyResolver()
                .resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }
}
