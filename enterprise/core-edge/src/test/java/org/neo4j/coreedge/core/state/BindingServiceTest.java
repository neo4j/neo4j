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
package org.neo4j.coreedge.core.state;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.core.state.storage.SimpleStorage;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.time.Clocks;

import static java.lang.Thread.sleep;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class BindingServiceTest
{
    private CoreTopologyService topologyService = mock( CoreTopologyService.class );
    private SimpleStorage<ClusterId> clusterIdStorage = mock( SimpleStorage.class );

    @Test
    public void shouldTimeoutEventually() throws Throwable
    {
        // given
        ClusterTopology topology = new ClusterTopology( null, false, emptyMap(), emptySet() );

        when( topologyService.currentTopology() ).thenReturn( topology );

        BindingService bindingService = new BindingService( clusterIdStorage, topologyService,
                getInstance(), Clocks.systemClock(), () -> sleep( 1 ), 50 );

        // when
        try
        {
            bindingService.start();
            fail();
        }
        catch ( TimeoutException e )
        {
            // then: expected
        }
    }

    @Test
    public void shouldConsiderTopologyChanges() throws Throwable
    {
        // given
        ClusterId commonClusterId = new ClusterId( UUID.randomUUID() );

        ClusterTopology topologyNOK = new ClusterTopology( null, false, emptyMap(), emptySet() );
        ClusterTopology topologyOK = new ClusterTopology( commonClusterId, false, emptyMap(), emptySet() );

        when( topologyService.currentTopology() ).thenReturn( topologyNOK, topologyNOK, topologyNOK, topologyOK );

        BindingService bindingService = new BindingService( clusterIdStorage, topologyService,
                getInstance(), Clocks.systemClock(), () -> sleep( 1 ), 30_000 );

        // when
        bindingService.start();

        // then
        assertEquals( commonClusterId, bindingService.clusterId() );
        verify( topologyService, never() ).publishClusterId( any() );
        verify( clusterIdStorage ).writeState( bindingService.clusterId() );
    }

    @Test
    public void shouldPublishNewId() throws Throwable
    {
        // given
        ClusterTopology topology = new ClusterTopology( null, true, emptyMap(), emptySet() );

        when( topologyService.currentTopology() ).thenReturn( topology );
        when( topologyService.publishClusterId( any() ) ).thenReturn( true );

        BindingService bindingService = new BindingService( clusterIdStorage, topologyService,
                getInstance(), Clocks.systemClock(), () -> sleep( 1 ), 30_000 );

        // when
        bindingService.start();

        // then
        verify( topologyService ).publishClusterId( bindingService.clusterId() );
        verify( clusterIdStorage ).writeState( bindingService.clusterId() );
    }

    @Test
    public void shouldPublishOldId() throws Throwable
    {
        // given
        ClusterTopology topology = new ClusterTopology( null, true, emptyMap(), emptySet() );
        ClusterId localClusterId = new ClusterId( UUID.randomUUID() );

        when( clusterIdStorage.exists() ).thenReturn( true );
        when( clusterIdStorage.readState() ).thenReturn( localClusterId );

        when( topologyService.currentTopology() ).thenReturn( topology );
        when( topologyService.publishClusterId( any() ) ).thenReturn( true );

        BindingService bindingService = new BindingService( clusterIdStorage, topologyService,
                getInstance(), Clocks.systemClock(), () -> sleep( 1 ), 30_000 );

        // when
        bindingService.start();

        // then
        assertEquals( localClusterId, bindingService.clusterId() );
        verify( topologyService ).publishClusterId( localClusterId );
        verify( clusterIdStorage, never() ).writeState( any() );
    }
}
