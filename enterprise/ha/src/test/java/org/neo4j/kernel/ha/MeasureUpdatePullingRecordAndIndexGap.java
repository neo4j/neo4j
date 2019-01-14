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
package org.neo4j.kernel.ha;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@Ignore( "Not a test, but a tool to measure an isolation characteristic where a change will be visible in an index "
        + "will see changes after being visible in the record store. This test tries to measure how big that gap is" )
public class MeasureUpdatePullingRecordAndIndexGap
{
    private final int numberOfIndexes = 10;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( HaSettings.tx_push_factor, "0" );

    @Test
    public void shouldMeasureThatGap() throws Exception
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        createIndexes( cluster.getMaster() );
        cluster.sync();
        awaitIndexes( cluster );
        final AtomicBoolean halter = new AtomicBoolean();
        AtomicLong[] highIdNodes = new AtomicLong[numberOfIndexes];
        CountDownLatch endLatch = new CountDownLatch( numberOfIndexes + 1 );
        for ( int i = 0; i < highIdNodes.length; i++ )
        {
            highIdNodes[i] = new AtomicLong();
        }
        startLoadOn( cluster.getMaster(), halter, highIdNodes, endLatch );
        GraphDatabaseAPI slave = cluster.getAnySlave();
        startCatchingUp( slave, halter, endLatch );

        // WHEN measuring...
        final AtomicInteger good = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        AtomicInteger ugly = new AtomicInteger();
        startMeasuringTheGap( good, bad, ugly, halter, highIdNodes, slave );
        for ( long endTime = currentTimeMillis() + SECONDS.toMillis( 30 ); currentTimeMillis() < endTime; )
        {
            printStats( good.get(), bad.get(), ugly.get() );
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        halter.set( true );
        endLatch.await();

        // THEN
        printStats( good.get(), bad.get(), ugly.get() );
    }

    private void startMeasuringTheGap( final AtomicInteger good, final AtomicInteger bad, final AtomicInteger ugly,
            final AtomicBoolean halter, final AtomicLong[] highIdNodes, final GraphDatabaseAPI db )
    {
        new Thread( () -> {
            while ( !halter.get() )
            {
                for ( int i = 0; i < numberOfIndexes; i++ )
                {
                    long targetNodeId = highIdNodes[i].get();
                    if ( targetNodeId == 0 )
                    {
                        continue;
                    }

                    try ( Transaction tx = db.beginTx() )
                    {
                        Node nodeFromStorePOV = null;
                        long nodeId = targetNodeId;
                        while ( nodeFromStorePOV == null && !halter.get() )
                        {
                            try
                            {
                                nodeFromStorePOV = db.getNodeById( nodeId );
                            }
                            catch ( NotFoundException e )
                            {   // Keep on spinning
                                nodeId = max( nodeId - 1, 0 );
                                try
                                {
                                    Thread.sleep( 10 );
                                }
                                catch ( InterruptedException e1 )
                                {
                                    throw new RuntimeException( e1 );
                                }
                            }
                        }

                        Node nodeFromIndexPOV = db.findNode( label( i ), key( i ), nodeId );
                        tx.success();
                        if ( nodeFromIndexPOV != null )
                        {
                            good.incrementAndGet();
                        }
                        else
                        {
                            bad.incrementAndGet();
                        }
                        if ( nodeId != targetNodeId )
                        {
                            ugly.incrementAndGet();
                        }
                    }
                }
            }
        } ).start();
    }

    private void printStats( int good, int bad, int ugly )
    {
        double total = good + bad;
        System.out.printf( "good: %.1f%%, bad: %.1f%%, ugly: %.1f%% (out of a total of %.0f)%n",
                100.0 * good / total,
                100.0 * bad / total,
                100.0 * ugly / total,
                total );
    }

    private void awaitIndexes( ManagedCluster cluster )
    {
        for ( GraphDatabaseService db : cluster.getAllMembers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, MINUTES );
                tx.success();
            }
        }
    }

    private void startCatchingUp( final GraphDatabaseAPI db, final AtomicBoolean halter, final CountDownLatch endLatch )
    {
        new Thread( () -> {
            try
            {
                while ( !halter.get() )
                {
                    try
                    {
                        db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException( e );
                    }
                }
            }
            finally
            {
                endLatch.countDown();
            }
        } ).start();
    }

    private void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numberOfIndexes; i++ )
            {
                db.schema().indexFor( label( i ) ).on( key( i ) ).create();
            }
            tx.success();
        }
    }

    private String key( int i )
    {
        return "key-" + i;
    }

    private Label label( int i )
    {
        return Label.label( "Label" + i );
    }

    private void startLoadOn( final GraphDatabaseService db, final AtomicBoolean halter, final AtomicLong[] highIdNodes,
            final CountDownLatch endLatch )
    {
        for ( int i = 0; i < numberOfIndexes; i++ )
        {
            final int x = i;
            new Thread( () -> {
                try
                {
                    while ( !halter.get() )
                    {
                        long nodeId;
                        try ( Transaction tx = db.beginTx() )
                        {
                            Node node = db.createNode( label( x ) );
                            node.setProperty( key( x ), nodeId = node.getId() );
                            tx.success();
                        }
                        highIdNodes[x].set( nodeId );
                    }
                }
                finally
                {
                    endLatch.countDown();
                }
            } ).start();
        }
    }
}
