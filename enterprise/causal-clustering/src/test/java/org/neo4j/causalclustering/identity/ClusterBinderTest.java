/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.identity;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.state.CoreBootstrapper;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterBinderTest
{
    private final CoreBootstrapper coreBootstrapper = mock( CoreBootstrapper.class );
    private final FakeClock clock = Clocks.fakeClock();

    @Test
    public void shouldTimeoutWhenNotBootrappableAndNobodyElsePublishesClusterId() throws Throwable
    {
        // given
        CoreTopology unboundTopology = new CoreTopology( null, false, emptyMap() );
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreServers() ).thenReturn( unboundTopology );

        ClusterBinder binder = new ClusterBinder( new StubClusterIdStorage(), topologyService,
                NullLogProvider.getInstance(), clock, () -> clock.forward( 1, TimeUnit.SECONDS ), 3_000,
                coreBootstrapper );

        try
        {
            // when
            binder.bindToCluster();
            fail( "Should have timed out" );
        }
        catch ( TimeoutException e )
        {
            // expected
        }

        // then
        verify( topologyService, atLeast( 2 ) ).coreServers();
    }

    @Test
    public void shouldBindToClusterIdPublishedByAnotherMember() throws Throwable
    {
        // given
        ClusterId publishedClusterId = new ClusterId( UUID.randomUUID() );
        CoreTopology unboundTopology = new CoreTopology( null, false, emptyMap() );
        CoreTopology boundTopology = new CoreTopology( publishedClusterId, false, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreServers() ).thenReturn( unboundTopology ).thenReturn( boundTopology );

        ClusterBinder binder = new ClusterBinder( new StubClusterIdStorage(), topologyService,
                NullLogProvider.getInstance(), clock, () -> clock.forward( 1, TimeUnit.SECONDS ), 3_000,
                coreBootstrapper );

        // when
        binder.bindToCluster();

        // then
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( publishedClusterId, clusterId.get() );
        verify( topologyService, atLeast( 2 ) ).coreServers();
    }

    @Test
    public void shouldPublishStoredClusterIdIfPreviouslyBound() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId ) ).thenReturn( true );

        StubClusterIdStorage clusterIdStorage = new StubClusterIdStorage();
        clusterIdStorage.writeState( previouslyBoundClusterId );

        ClusterBinder binder = new ClusterBinder( clusterIdStorage, topologyService,
                NullLogProvider.getInstance(), clock, () -> clock.forward( 1, TimeUnit.SECONDS ), 3_000,
                coreBootstrapper );

        // when
        binder.bindToCluster();

        // then
        verify( topologyService ).setClusterId( previouslyBoundClusterId );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        assertEquals( previouslyBoundClusterId, clusterId.get() );
    }

    @Test
    public void shouldFailToPublishMismatchingStoredClusterId() throws Throwable
    {
        // given
        ClusterId previouslyBoundClusterId = new ClusterId( UUID.randomUUID() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.setClusterId( previouslyBoundClusterId ) ).thenReturn( false );

        StubClusterIdStorage clusterIdStorage = new StubClusterIdStorage();
        clusterIdStorage.writeState( previouslyBoundClusterId );

        ClusterBinder binder = new ClusterBinder( clusterIdStorage, topologyService,
                NullLogProvider.getInstance(), clock, () -> clock.forward( 1, TimeUnit.SECONDS ), 3_000,
                coreBootstrapper );

        // when
        try
        {
            binder.bindToCluster();
            fail( "Should have thrown exception" );
        }
        catch ( BindingException e )
        {
            // expected
        }
    }

    @Test
    public void shouldBootstrapWhenBootstrappable() throws Throwable
    {
        // given
        CoreTopology bootstrappableTopology = new CoreTopology( null, true, emptyMap() );

        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        when( topologyService.coreServers() ).thenReturn( bootstrappableTopology );
        when( topologyService.setClusterId( any() ) ).thenReturn( true );
        CoreSnapshot snapshot = mock( CoreSnapshot.class );
        when( coreBootstrapper.bootstrap( any() ) ).thenReturn( snapshot );

        ClusterBinder binder = new ClusterBinder( new StubClusterIdStorage(), topologyService,
                NullLogProvider.getInstance(), clock, () -> clock.forward( 1, TimeUnit.SECONDS ),
                3_000, coreBootstrapper );

        // when
        BoundState boundState = binder.bindToCluster();

        // then
        verify( coreBootstrapper ).bootstrap( any() );
        Optional<ClusterId> clusterId = binder.get();
        assertTrue( clusterId.isPresent() );
        verify( topologyService ).setClusterId( clusterId.get() );
        assertTrue( boundState.snapshot().isPresent() );
        assertEquals( boundState.snapshot().get(), snapshot );
    }

    private class StubClusterIdStorage implements SimpleStorage<ClusterId>
    {
        private ClusterId clusterId;

        @Override
        public boolean exists()
        {
            return clusterId != null;
        }

        @Override
        public ClusterId readState() throws IOException
        {
            return clusterId;
        }

        @Override
        public void writeState( ClusterId state ) throws IOException
        {
            clusterId = state;
        }
    }
}
