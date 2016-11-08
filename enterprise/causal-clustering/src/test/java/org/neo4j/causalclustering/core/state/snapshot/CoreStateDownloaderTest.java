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
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFetcher;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
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
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LocalDatabase localDatabase = mock( LocalDatabase.class );
    private final Lifecycle startStopLife = mock( Lifecycle.class );
    private final StoreFetcher storeFetcher = mock( StoreFetcher.class );
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );
    private final CopiedStoreRecovery recovery = mock( CopiedStoreRecovery.class );
    private final CoreState coreState = mock( CoreState.class );

    private final NullLogProvider logProvider = NullLogProvider.getInstance();

    private final MemberId remoteMember = new MemberId( UUID.randomUUID() );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final File storeDir = new File( "graph.db" );
    private final File tempDir = new File( "graph.db/temp-copy" );

    private final CoreStateDownloader downloader =
            new CoreStateDownloader( fs, localDatabase, startStopLife, storeFetcher, catchUpClient, logProvider,
                    recovery );

    @Before
    public void commonMocking()
    {
        when( localDatabase.storeId() ).thenReturn( storeId );
        when( localDatabase.storeDir() ).thenReturn( storeDir );
    }

    @Test
    public void shouldDownloadCompleteStoreWhenEmpty() throws Throwable
    {
        // given
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( storeFetcher.getStoreIdOf( remoteMember ) ).thenReturn( remoteStoreId );
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( remoteMember, coreState );

        // then
        verify( storeFetcher, never() ).tryCatchingUp( any(), any(), any() );
        verify( storeFetcher ).copyStore( remoteMember, remoteStoreId, tempDir );
        verify( localDatabase ).replaceWith( tempDir );
    }

    @Test
    public void shouldStopDatabaseDuringDownload() throws Throwable
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( true );

        // when
        downloader.downloadSnapshot( remoteMember, coreState );

        // then
        verify( startStopLife ).stop();
        verify( localDatabase ).stop();
        verify( localDatabase ).start();
        verify( startStopLife ).start();
    }

    @Test
    public void shouldNotOverwriteNonEmptyMismatchingStore() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        StoreId remoteStoreId = new StoreId( 5, 6, 7, 8 );
        when( storeFetcher.getStoreIdOf( remoteMember ) ).thenReturn( remoteStoreId );

        // when
        try
        {
            downloader.downloadSnapshot( remoteMember, coreState );
            fail();
        }
        catch ( StoreCopyFailedException e )
        {
            // expected
        }

        // then
        verify( storeFetcher, never() ).copyStore( any(), any(), any() );
        verify( storeFetcher, never() ).tryCatchingUp( any(), any(), any() );
    }

    @Test
    public void shouldCatchupIfPossible() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( storeFetcher.getStoreIdOf( remoteMember ) ).thenReturn( storeId );
        when( storeFetcher.tryCatchingUp( remoteMember, storeId, storeDir ) ).thenReturn( SUCCESS_END_OF_STREAM );

        // when
        downloader.downloadSnapshot( remoteMember, coreState );

        // then
        verify( storeFetcher ).tryCatchingUp( remoteMember, storeId, storeDir );
        verify( storeFetcher, never() ).copyStore( any(), any(), any() );
    }

    @Test
    public void shouldDownloadWholeStoreIfCannotCatchUp() throws Exception
    {
        // given
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( storeFetcher.getStoreIdOf( remoteMember ) ).thenReturn( storeId );
        when( storeFetcher.tryCatchingUp( remoteMember, storeId, storeDir ) ).thenReturn( E_TRANSACTION_PRUNED );

        // when
        downloader.downloadSnapshot( remoteMember, coreState );

        // then
        verify( storeFetcher ).tryCatchingUp( remoteMember, storeId, storeDir );
        verify( storeFetcher ).copyStore( remoteMember, storeId, tempDir );
        verify( localDatabase ).replaceWith( tempDir );
    }
}
