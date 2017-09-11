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
package org.neo4j.causalclustering.readreplica;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadReplicaStartupProcessTest
{
    private ConstantTimeTimeoutStrategy retryStrategy = new ConstantTimeTimeoutStrategy( 1, MILLISECONDS );
    private StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
    private RemoteStore remoteStore = mock( RemoteStore.class );
    private final PageCache pageCache = mock( PageCache.class );
    private LocalDatabase localDatabase = mock( LocalDatabase.class );
    private TopologyService topologyService = mock( TopologyService.class );
    private CoreTopology clusterTopology = mock( CoreTopology.class );
    private Lifecycle txPulling = mock( Lifecycle.class );

    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "127.0.0.1", 123 );
    private StoreId localStoreId = new StoreId( 1, 2, 3, 4 );
    private StoreId otherStoreId = new StoreId( 5, 6, 7, 8 );
    private File storeDir = new File( "store-dir" );

    @Before
    public void commonMocking() throws StoreIdDownloadFailedException, IOException
    {
        Map<MemberId,CoreServerInfo> members = new HashMap<>();
        members.put( memberId, mock( CoreServerInfo.class ) );

        FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
        when( fileSystemAbstraction.streamFilesRecursive( any( File.class ) ) ).thenAnswer( f -> Stream.empty() );
        when( pageCache.getCachedFileSystem() ).thenReturn( fileSystemAbstraction );
        when( localDatabase.storeDir() ).thenReturn( storeDir );
        when( localDatabase.storeId() ).thenReturn( localStoreId );
        when( topologyService.coreServers() ).thenReturn( clusterTopology );
        when( clusterTopology.members() ).thenReturn( members );
        when( topologyService.findCatchupAddress( memberId ) ).thenReturn( Optional.of( fromAddress ) );
    }

    @Test
    public void shouldReplaceEmptyStoreWithRemote() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );
        when( topologyService.findCatchupAddress( any() )).thenReturn( Optional.of( fromAddress ) );
        when( remoteStore.getStoreId( any() ) ).thenReturn( otherStoreId );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, chooseFirstMember(), retryStrategy, NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), storeCopyProcess, topologyService );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( storeCopyProcess ).replaceWithStoreFrom( any(), any() );
        verify( localDatabase ).start();
        verify( txPulling ).start();
    }

    private UpstreamDatabaseStrategySelector chooseFirstMember()
    {
        AlwaysChooseFirstMember firstMember = new AlwaysChooseFirstMember();
        firstMember.inject( topologyService, null, NullLogProvider.getInstance(), null);

        return new UpstreamDatabaseStrategySelector( firstMember );
    }

    @Test
    public void shouldNotStartWithMismatchedNonEmptyStore() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( any() ) ).thenReturn( otherStoreId );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, chooseFirstMember(), retryStrategy, NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), storeCopyProcess, topologyService );

        // when
        try
        {
            readReplicaStartupProcess.start();
            fail( "should have thrown" );
        }
        catch ( Exception ex )
        {
            //expected.
            assertThat( ex.getMessage(),
                    containsString( "This read replica cannot join the cluster. The local database is not empty and has a " + "mismatching storeId" ) );
        }

        // then
        verify( txPulling, never() ).start();
    }

    @Test
    public void shouldStartWithMatchingDatabase() throws Throwable
    {
        // given
        when( remoteStore.getStoreId( any() ) ).thenReturn( localStoreId );
        when( localDatabase.isEmpty() ).thenReturn( false );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, chooseFirstMember(), retryStrategy, NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), storeCopyProcess, topologyService );

        // when
        readReplicaStartupProcess.start();

        // then
        verify( localDatabase ).start();
        verify( txPulling ).start();
    }

    @Test
    public void stopShouldStopTheDatabaseAndStopPolling() throws Throwable
    {
        // given
        when( remoteStore.getStoreId( any() ) ).thenReturn( localStoreId );
        when( localDatabase.isEmpty() ).thenReturn( false );

        ReadReplicaStartupProcess readReplicaStartupProcess =
                new ReadReplicaStartupProcess( remoteStore, localDatabase, txPulling, chooseFirstMember(), retryStrategy, NullLogProvider.getInstance(),
                        NullLogProvider.getInstance(), storeCopyProcess, topologyService );

        readReplicaStartupProcess.start();

        // when
        readReplicaStartupProcess.stop();

        // then
        verify( txPulling ).stop();
        verify( localDatabase ).stop();
    }

    @Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
    public static class AlwaysChooseFirstMember extends UpstreamDatabaseSelectionStrategy
    {
        public AlwaysChooseFirstMember()
        {
            super( "always-choose-first-member" );
        }

        @Override
        public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
        {
            CoreTopology coreTopology = topologyService.coreServers();
            return Optional.ofNullable( coreTopology.members().keySet().iterator().next() );
        }
    }
}
