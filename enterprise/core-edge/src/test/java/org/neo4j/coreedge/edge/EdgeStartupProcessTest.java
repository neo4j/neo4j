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
package org.neo4j.coreedge.edge;

import java.util.UUID;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.core.state.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.EdgeTopologyService;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.AlwaysChooseFirstMember;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class EdgeStartupProcessTest
{
    @Test
    public void startShouldReplaceTheEmptyLocalStoreWithStoreFromCoreMemberAndStartPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = Mockito.mock( StoreFetcher.class );
        LocalDatabase localDatabase = Mockito.mock( LocalDatabase.class );

        MemberId memberId = new MemberId( UUID.randomUUID() );
        TopologyService hazelcastTopology = Mockito.mock( TopologyService.class );

        ClusterTopology clusterTopology = Mockito.mock( ClusterTopology.class );
        Mockito.when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        Mockito.when( clusterTopology.coreMembers() ).thenReturn( asSet( memberId ) );
        Mockito.when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = Mockito.mock( DataSourceManager.class );
        Lifecycle txPulling = Mockito.mock( Lifecycle.class );

        EdgeStartupProcess edgeStartupProcess = new EdgeStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstMember( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                Mockito.mock( EdgeTopologyService.class ), Config.empty() );

        // when
        edgeStartupProcess.start();

        // then
        Mockito.verify( dataSourceManager ).start();
        Mockito.verify( localDatabase ).isEmpty();
        Mockito.verify( localDatabase ).stop();
        Mockito.verify( localDatabase ).copyStoreFrom( memberId, storeFetcher );
        Mockito.verify( localDatabase ).start();
        Mockito.verify( txPulling ).start();
        Mockito.verifyNoMoreInteractions( localDatabase, dataSourceManager, txPulling );
    }

    @Test
    public void startShouldNotReplaceTheNonEmptyLocalStoreWithStoreFromCoreMemberAndStartPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = Mockito.mock( StoreFetcher.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        LocalDatabase localDatabase = Mockito.mock( LocalDatabase.class );
        Mockito.when( localDatabase.isEmpty() ).thenReturn( false );
        Mockito.doThrow( IllegalStateException.class ).when( localDatabase ).ensureSameStoreId( memberId, storeFetcher );

        TopologyService hazelcastTopology = Mockito.mock( TopologyService.class );

        ClusterTopology clusterTopology = Mockito.mock( ClusterTopology.class );
        Mockito.when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        Mockito.when( clusterTopology.coreMembers() ).thenReturn( asSet( memberId ) );

        DataSourceManager dataSourceManager = Mockito.mock( DataSourceManager.class );
        Lifecycle txPulling = Mockito.mock( Lifecycle.class );

        EdgeStartupProcess edgeStartupProcess = new EdgeStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstMember( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                Mockito.mock( EdgeTopologyService.class ), Config.empty() );

        // when
        try
        {
            edgeStartupProcess.start();
            fail( "should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // expected
        }

        // then
        Mockito.verify( dataSourceManager ).start();
        Mockito.verify( localDatabase ).isEmpty();
        Mockito.verify( localDatabase ).ensureSameStoreId( memberId, storeFetcher );
        Mockito.verifyNoMoreInteractions( localDatabase, dataSourceManager );
        Mockito.verifyZeroInteractions( txPulling );
    }

    @Test
    public void startShouldSimplyStartPollingOnNonEmptyDatabaseAndMatchingStoreId() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        StoreFetcher storeFetcher = Mockito.mock( StoreFetcher.class );
        Mockito.when( storeFetcher.storeId( Matchers.any( MemberId.class ) ) ).thenReturn( storeId );
        LocalDatabase localDatabase = Mockito.mock( LocalDatabase.class );
        Mockito.when( localDatabase.storeId() ).thenReturn( storeId );

        MemberId memberId = new MemberId( UUID.randomUUID() );
        TopologyService hazelcastTopology = Mockito.mock( TopologyService.class );

        ClusterTopology clusterTopology = Mockito.mock( ClusterTopology.class );
        Mockito.when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );

        Mockito.when( clusterTopology.coreMembers() ).thenReturn( asSet( memberId ) );
        Mockito.when( localDatabase.isEmpty() ).thenReturn( false );

        DataSourceManager dataSourceManager = Mockito.mock( DataSourceManager.class );
        Lifecycle txPulling = Mockito.mock( Lifecycle.class );

        EdgeStartupProcess edgeStartupProcess = new EdgeStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstMember( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                Mockito.mock( EdgeTopologyService.class ), Config.empty() );

        // when
        edgeStartupProcess.start();

        // then
        Mockito.verify( localDatabase ).isEmpty();
        Mockito.verify( localDatabase ).ensureSameStoreId( memberId, storeFetcher );
        Mockito.verify( dataSourceManager ).start();
        Mockito.verify( txPulling ).start();
        Mockito.verifyNoMoreInteractions( localDatabase, dataSourceManager, txPulling );
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        StoreFetcher storeFetcher = Mockito.mock( StoreFetcher.class );
        LocalDatabase localDatabase = Mockito.mock( LocalDatabase.class );

        MemberId memberId = new MemberId( UUID.randomUUID() );
        TopologyService hazelcastTopology = Mockito.mock( TopologyService.class );
        ClusterTopology clusterTopology = Mockito.mock( ClusterTopology.class );
        Mockito.when( clusterTopology.coreMembers() ).thenReturn( asSet( memberId ) );

        Mockito.when( hazelcastTopology.currentTopology() ).thenReturn( clusterTopology );
        Mockito.when( localDatabase.isEmpty() ).thenReturn( true );

        DataSourceManager dataSourceManager = Mockito.mock( DataSourceManager.class );
        Lifecycle txPulling = Mockito.mock( Lifecycle.class );
        EdgeStartupProcess edgeStartupProcess = new EdgeStartupProcess( storeFetcher, localDatabase,
                txPulling, dataSourceManager, new AlwaysChooseFirstMember( hazelcastTopology ),
                new ConstantTimeRetryStrategy( 1, MILLISECONDS ), NullLogProvider.getInstance(),
                Mockito.mock( EdgeTopologyService.class ), null );

        // when
        edgeStartupProcess.stop();

        // then
        Mockito.verify( txPulling ).stop();
        Mockito.verify( dataSourceManager ).stop();
        Mockito.verifyNoMoreInteractions( txPulling, dataSourceManager );
    }
}
