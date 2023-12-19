/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.CommitStateHelper;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class CoreStateDownloaderTest
{
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final Suspendable startStopLife = mock( Suspendable.class );
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

    @Before
    public void commonMocking()
    {
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( localDatabase.storeDir() ).thenReturn( storeDir );
        when( topologyService.findCatchupAddress( remoteMember ) ).thenReturn( Optional.of( remoteAddress ) );
    }

    @Test
    public void shouldDownloadCompleteStoreWhenEmpty() throws Throwable
    {
        // given
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( remoteStoreId );
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
        verify( storeCopyProcess ).replaceWithStoreFrom( catchupAddressProvider, remoteStoreId );
    }

    @Test
    public void shouldStopDatabaseDuringDownload() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( startStopLife ).disable();
        verify( localDatabase ).stopForStoreCopy();
        verify( localDatabase ).start();
        verify( startStopLife ).enable();
    }

    @Test
    public void shouldNotOverwriteNonEmptyMismatchingStore() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( remoteStoreId );

        // when
        assertFalse( downloader.downloadSnapshot( catchupAddressProvider ) );

        // then
        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any(), anyBoolean(), anyBoolean() );
    }

    @Test
    public void shouldCatchupIfPossible() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir, false, false ) ).thenReturn( SUCCESS_END_OF_STREAM );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir, false, false );
        verify( remoteStore, never() ).copy( any(), any(), any(), anyBoolean() );
    }

    @Test
    public void shouldDownloadWholeStoreIfCannotCatchUp() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir, false, false ) ).thenReturn( E_TRANSACTION_PRUNED );

        // when
        downloader.downloadSnapshot( catchupAddressProvider );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir, false, false );
        verify( storeCopyProcess ).replaceWithStoreFrom( catchupAddressProvider, storeId );
    }
}
