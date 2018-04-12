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

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmerMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PageCacheWarmupCcIT extends PageCacheWarmupTestSupport
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedCoreParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" )
            .withSharedReadReplicaParam( UdcSettings.udc_enabled, Settings.FALSE )
            .withSharedReadReplicaParam( GraphDatabaseSettings.pagecache_warmup_profiling_interval, "100ms" );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    private long warmUpCluster() throws TimeoutException
    {
        CoreClusterMember leader = cluster.awaitLeader();
        createTestData( leader.database() );
        long pagesInMemory = waitForCacheProfile( leader.database() );
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            waitForCacheProfile( member.database() );
        }
        return pagesInMemory;
    }

    private void verifyWarmupHappensAfterStoreCopy( ClusterMember member, long pagesInMemory )
    {
        AtomicLong pagesLoadedInWarmup = new AtomicLong();
        BinaryLatch warmupLatch = new BinaryLatch();
        Monitors monitors = member.monitors();
        monitors.addMonitorListener( new PageCacheWarmerMonitor()
        {
            @Override
            public void warmupCompleted( long elapsedMillis, long pagesLoaded )
            {
                pagesLoadedInWarmup.set( pagesInMemory );
                warmupLatch.release();
            }

            @Override
            public void profileCompleted( long elapsedMillis, long pagesInMemory )
            {
            }
        } );
        member.start();
        warmupLatch.await();
        assertThat( pagesLoadedInWarmup.get(), is( pagesInMemory ) );
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToCore() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        ClusterMember member = cluster.addCoreMemberWithId( 4 );
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }

    @Test
    public void cacheProfilesMustBeIncludedInStoreCopyToReadReplica() throws Exception
    {
        long pagesInMemory = warmUpCluster();
        ClusterMember member = cluster.addReadReplicaWithId( 4 );
        verifyWarmupHappensAfterStoreCopy( member, pagesInMemory );
    }
}
