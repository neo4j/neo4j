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
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class CoreStateDownloaderTest
{
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final Lifecycle startStopLife = mock( Lifecycle.class );
    private final RemoteStore remoteStore = mock( RemoteStore.class );
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final StoreCopyProcess storeCopyProcess = mock( StoreCopyProcess.class );
    private CoreSnapshotService snaptshotService = mock( CoreSnapshotService.class );
    private CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
    private TopologyService topologyService = mock( TopologyService.class );

    private final CoreStateMachines coreStateMachines = mock( CoreStateMachines.class );

    private final NullLogProvider logProvider = NullLogProvider.getInstance();

    private final MemberId remoteMember = new MemberId( UUID.randomUUID() );
    private final AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "remoteAddress", 1234 );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final File storeDir = new File( "graph.db" );

    private final CoreStateDownloader downloader =
            new CoreStateDownloader( localDatabase, startStopLife, remoteStore, catchUpClient, logProvider, storeCopyProcess, coreStateMachines,
                    snaptshotService, applicationProcess, topologyService );

    @Before
    public void commonMocking() throws IOException
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
        downloader.downloadSnapshot( remoteMember );

        // then
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any() );
        verify( storeCopyProcess ).replaceWithStoreFrom( remoteAddress, remoteStoreId );
    }

    @Test
    public void shouldStopDatabaseDuringDownload() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( remoteMember );

        // then
        verify( startStopLife ).stop();
        verify( localDatabase ).stopForStoreCopy();
        verify( localDatabase ).start();
        verify( startStopLife ).start();
    }

    @Test
    public void shouldNotOverwriteNonEmptyMismatchingStore() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( remoteStoreId );

        // when
        try
        {
            downloader.downloadSnapshot( remoteMember );
            fail();
        }
        catch ( StoreCopyFailedException e )
        {
            // expected
        }

        // then
        verify( remoteStore, never() ).copy( any(), any(), any() );
        verify( remoteStore, never() ).tryCatchingUp( any(), any(), any() );
    }

    @Test
    public void shouldCatchupIfPossible() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir ) ).thenReturn( SUCCESS_END_OF_STREAM );

        // when
        downloader.downloadSnapshot( remoteMember );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir );
        verify( remoteStore, never() ).copy( any(), any(), any() );
    }

    @Test
    public void shouldDownloadWholeStoreIfCannotCatchUp() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( remoteStore.getStoreId( remoteAddress ) ).thenReturn( storeId );
        when( remoteStore.tryCatchingUp( remoteAddress, storeId, storeDir ) ).thenReturn( E_TRANSACTION_PRUNED );

        // when
        downloader.downloadSnapshot( remoteMember );

        // then
        verify( remoteStore ).tryCatchingUp( remoteAddress, storeId, storeDir );
        verify( storeCopyProcess ).replaceWithStoreFrom( remoteAddress, storeId );
    }
}
