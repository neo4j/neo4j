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
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.CommitStateHelper;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

class CoreStateDownloaderTest
{
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final Lifecycle startStopLife = mock( Lifecycle.class );
    private final RemoteStore remoteStore = mock( RemoteStore.class );
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
    private CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
    private TopologyService topologyService = mock( TopologyService.class );
    private CommitStateHelper commitStateHelper = mock( CommitStateHelper.class );
    private final CoreStateMachines coreStateMachines = mock( CoreStateMachines.class );

    private final NullLogProvider logProvider = NullLogProvider.getInstance();

    private final MemberId remoteMember = new MemberId( UUID.randomUUID() );
    private final AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "remoteAddress", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( remoteAddress );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final File storeDir = new File( "graph.db" );

    private final CoreStateDownloader downloader =
            new CoreStateDownloader( localDatabase, startStopLife, remoteStore, catchUpClient, logProvider, storeCopyProcess, coreStateMachines,
                    snapshotService, commitStateHelper );

    @BeforeEach
    void commonMocking()
    {
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( localDatabase.storeDir() ).thenReturn( storeDir );
        when( topologyService.findCatchupAddress( remoteMember ) ).thenReturn( Optional.of( remoteAddress ) );
    }

    @Test
    void shouldDownloadCompleteStoreWhenEmpty() throws Throwable
    {
        // given
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( remoteStoreId );
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( catchupAddressProvider, remoteStoreId );
    }

    @Test
    void shouldStopDatabaseDuringDownload() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( startStopLife ).stop();
        verify( localDatabase ).stopForStoreCopy();
        verify( localDatabase ).start();
        verify( startStopLife ).start();
    }

    @Test
    void shouldNotOverwriteNonEmptyMismatchingStore() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( remoteStoreId );

        // when
        try
        {
            downloader.downloadSnapshot( catchupAddressProvider );
            fail( "Failure was expected" );
        }
        catch ( StoreCopyFailedException e )
        {
            // expected
        }

        // then
        verify( remoteStore, never() ).copy( any(), any(), any() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean() );
    }

    @Test
    void shouldCatchupIfPossible() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir, false ) ).thenReturn( SUCCESS_END_OF_STREAM );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir, false );
        verify( remoteStore, never() ).copy( any(), any(), any() );
    }

    @Test
    void shouldDownloadWholeStoreIfCannotCatchUp() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir, false ) ).thenReturn( E_TRANSACTION_PRUNED );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir, false );
        verify( storeCopyProcess ).replaceWithStoreFrom( catchupAddressProvider, storeId );
    }
}
