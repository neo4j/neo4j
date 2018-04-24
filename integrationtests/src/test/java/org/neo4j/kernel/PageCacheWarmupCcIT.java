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
package org.neo4j.kernel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.upstream.strategies.LeaderOnlyStrategy;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmerMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class PageCacheWarmupCcIT extends PageCacheWarmupTestSupport
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedCoreParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedCoreParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY )
            // Restored to default value to decrease the risk of leader changes:
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "7s" )
            .withSharedReadReplicaParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedReadReplicaParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedReadReplicaParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY );

    private Cluster cluster;
    private AtomicReference<CoreGraphDatabase> leaderRef;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
        leaderRef = new AtomicReference<>();
    }

    private long warmUpCluster() throws Exception
    {
        cluster.awaitLeader(); // Make sure we have a cluster leader.
        cluster.coreTx( ( db, tx ) ->
        {
            // Verify that we really do have a somewhat stable leader.
            db.createNode();
            tx.success();
        } );
        cluster.coreTx( ( db, tx ) ->
        {
            // Alright, assuming this leader holds up, create the test data.
            leaderRef.set( db );
            createTestData( db );
            tx.success();
        } );
        AtomicLong pagesInMemory = new AtomicLong();
        cluster.coreTx( ( db, tx ) ->
        {
            // Now we can wait for the profile on the leader.
            pagesInMemory.set( waitForCacheProfile( db ) );
            // Make sure that this is still the same leader that we profiled:
            assumeLeaderUnchanged( db );
        } );
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            waitForCacheProfile( member.database() );
        }
        return pagesInMemory.get();
    }

    private void assumeLeaderUnchanged( CoreGraphDatabase db )
    {
        assumeThat( leaderRef.get(), sameInstance( db ) );
    }

    private void verifyWarmupHappensAfterStoreCopy( ClusterMember member, long pagesInMemory ) throws Exception
    {
        AtomicLong pagesLoadedInWarmup = new AtomicLong();
        BinaryLatch warmupLatch = injectWarmupLatch( member, pagesLoadedInWarmup );
        member.start();
        warmupLatch.await();
        // First make sure that the leader hasn't changed:
        cluster.coreTx( ( db, tx ) -> assumeLeaderUnchanged( db ) );
        // Then check that we warmup up all right:
        assertThat( pagesLoadedInWarmup.get(), greaterThanOrEqualTo( pagesInMemory ) );
    }

    private BinaryLatch injectWarmupLatch( ClusterMember member, AtomicLong pagesLoadedInWarmup )
    {
        BinaryLatch warmupLatch = new BinaryLatch();
        Monitors monitors = member.monitors();
        monitors.addMonitorListener( new PageCacheWarmerMonitor()
        {
            @Override
            public void warmupCompleted( long pagesLoaded )
            {
                pagesLoadedInWarmup.set( pagesLoaded );
                warmupLatch.release();
            }

            @Override
            public void profileCompleted( long pagesInMemory )
            {
            }
        } );
        return warmupLatch;
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToCore() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        CoreClusterMember member = cluster.newCoreMember();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToReadReplica() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        ReadReplica member = cluster.newReadReplica();
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }
}
