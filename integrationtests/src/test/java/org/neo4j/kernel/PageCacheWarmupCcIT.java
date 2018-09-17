/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.upstream.strategies.LeaderOnlyStrategy;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitorAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class PageCacheWarmupCcIT extends PageCacheWarmupTestSupport
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedCoreParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedCoreParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY )
            .withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, id -> id == 0 ? "false" : "true" )
            .withSharedReadReplicaParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedReadReplicaParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, Settings.TRUE )
            .withSharedReadReplicaParam( CausalClusteringSettings.pull_interval, "100ms" )
            .withSharedReadReplicaParam( CausalClusteringSettings.upstream_selection_strategy, LeaderOnlyStrategy.IDENTITY );

    private Cluster<?> cluster;
    private CoreClusterMember leader;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    private long warmUpCluster() throws Exception
    {
        leader = cluster.awaitLeader(); // Make sure we have a cluster leader.
        cluster.coreTx( ( db, tx ) ->
        {
            // Create some test data to touch a bunch of pages.
            createTestData( db );
            tx.success();
        } );
        AtomicLong pagesInMemory = new AtomicLong();
        cluster.coreTx( ( db, tx ) ->
        {
            // Wait for an initial profile on the leader. This profile might have raced with the 'createTestData'
            // transaction above, so it might be incomplete.
            waitForCacheProfile( leader.monitors() );
            // Now we can wait for a clean profile on the leader, and note the count for verifying later.
            pagesInMemory.set( waitForCacheProfile( leader.monitors() ) );
        } );
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            waitForCacheProfile( member.monitors() );
        }
        return pagesInMemory.get();
    }

    private static void verifyWarmupHappensAfterStoreCopy( ClusterMember member, long pagesInMemory )
    {
        AtomicLong pagesLoadedInWarmup = new AtomicLong();
        BinaryLatch warmupLatch = injectWarmupLatch( member, pagesLoadedInWarmup );
        member.start();
        warmupLatch.await();
        // Check that we warmup up all right:
        assertThat( pagesLoadedInWarmup.get(), greaterThanOrEqualTo( pagesInMemory ) );
    }

    private static BinaryLatch injectWarmupLatch( ClusterMember member, AtomicLong pagesLoadedInWarmup )
    {
        BinaryLatch warmupLatch = new BinaryLatch();
        Monitors monitors = member.monitors();
        monitors.addMonitorListener( new PageCacheWarmerMonitorAdapter()
        {
            @Override
            public void warmupCompleted( long pagesLoaded )
            {
                pagesLoadedInWarmup.set( pagesLoaded );
                warmupLatch.release();
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
