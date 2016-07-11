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
package org.neo4j.coreedge;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.coreedge.server.edge.AlwaysChooseFirstServer;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class EdgeServerStartupProcessTest
{
    @Test
    public void startShouldReplaceTheEmptyLocalStoreWithStoreFromCoreServerAndStartPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        TopologyService hazelcastTopology = mock( TopologyService.class );

        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );

        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), Config.empty() );

        // when
        edgeServerStartupProcess.start();

        // then
        verify( dataSourceManager ).start();
        verify( localDatabase ).isEmpty();
        verify( localDatabase ).stop();
        verify( localDatabase ).copyStoreFrom( coreMember, storeFetcher );
        verify( localDatabase ).start();
        verify( txPulling ).start();
        verifyNoMoreInteractions( localDatabase, dataSourceManager, txPulling );
    }

    @Test
    public void startShouldNotReplaceTheNonEmptyLocalStoreWithStoreFromCoreServerAndStartPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        LocalDatabase localDatabase = mock( LocalDatabase.class );
        when( localDatabase.isEmpty() ).thenReturn( false );
        doThrow( IllegalStateException.class ).when( localDatabase ).ensureSameStoreId( coreMember, storeFetcher );

        TopologyService hazelcastTopology = mock( TopologyService.class );

        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );

        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), Config.empty() );

        // when
        try
        {
            edgeServerStartupProcess.start();
            fail( "should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // expected
        }

        // then
        verify( dataSourceManager ).start();
        verify( localDatabase ).isEmpty();
        verify( localDatabase ).ensureSameStoreId( coreMember, storeFetcher );
        verifyNoMoreInteractions( localDatabase, dataSourceManager );
        verifyZeroInteractions( txPulling );
    }

    @Test
    public void startShouldSimplyStartPollingOnNonEmptyDatabaseAndMatchingStoreId() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        when( storeFetcher.storeId( any( CoreMember.class ) ) ).thenReturn( storeId );
        LocalDatabase localDatabase = mock( LocalDatabase.class );
        when( localDatabase.storeId() ).thenReturn( storeId );

        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        TopologyService hazelcastTopology = mock( TopologyService.class );

        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );
        when( localDatabase.isEmpty() ).thenReturn( false );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );

        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), Config.empty() );

        // when
        edgeServerStartupProcess.start();

        // then
        verify( localDatabase ).isEmpty();
        verify( localDatabase ).ensureSameStoreId( coreMember, storeFetcher );
        verify( dataSourceManager ).start();
        verify( txPulling ).start();
        verifyNoMoreInteractions( localDatabase, dataSourceManager, txPulling );
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        LocalDatabase localDatabase = mock( LocalDatabase.class );

        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        TopologyService hazelcastTopology = mock( TopologyService.class );
        ClusterTopology clusterTopology = mock( ClusterTopology.class );
        when( clusterTopology.coreMembers() ).thenReturn( asSet( coreMember ) );

        when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        Lifecycle txPulling = mock( Lifecycle.class );
        EdgeServerStartupProcess edgeServerStartupProcess = new EdgeServerStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstServer( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                mock( EdgeTopologyService.class ), null );

        // when
        edgeServerStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( dataSourceManager ).stop();
        verifyNoMoreInteractions( txPulling, dataSourceManager );
    }
}
