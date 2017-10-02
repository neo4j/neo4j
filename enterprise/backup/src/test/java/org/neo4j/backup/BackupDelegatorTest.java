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
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StreamingTransactionsFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupDelegatorTest
{
    private RemoteStore remoteStore;
    private CatchUpClient catchUpClient;
    private StoreCopyClient storeCopyClient;
    private ClearIdService clearIdService;

    BackupDelegator subject;

    private final AdvertisedSocketAddress anyAddress = new AdvertisedSocketAddress( "any.address", 1234 );

    @Before
    public void setup()
    {
        remoteStore = mock( RemoteStore.class );
        catchUpClient = mock( CatchUpClient.class );
        storeCopyClient = mock( StoreCopyClient.class );
        clearIdService = mock( ClearIdService.class );
        subject = new BackupDelegator( remoteStore, catchUpClient, storeCopyClient, clearIdService );
    }

    @Test
    public void tryCatchingUpDelegatesToRemoteStore() throws org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException, IOException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4j.com", 5432 );
        StoreId expectedStoreId = new StoreId( 7, 2, 5, 98 );
        File storeDir = new File( "A directory to store transactions to" );

        // when
        subject.tryCatchingUp( fromAddress, expectedStoreId, storeDir );

        // then
        verify( remoteStore ).tryCatchingUp( fromAddress, expectedStoreId, storeDir );
    }

    @Test
    public void startDelegatesToCatchUpClient()
    {
        // when
        subject.start();

        // then
        verify( catchUpClient ).start();
    }

    @Test
    public void stopDelegatesToCatchUpClient()
    {
        // when
        subject.stop();

        // then
        verify( catchUpClient ).stop();
    }

    @Test
    public void fetchStoreIdDelegatesToStoreCopyClient() throws StoreIdDownloadFailedException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4.com", 935 );

        // and
        StoreId expectedStoreId = new StoreId( 6, 2, 9, 3 );
        stub( storeCopyClient.fetchStoreId( fromAddress ) ).toReturn( expectedStoreId );

        // when
        StoreId storeId = subject.fetchStoreId( fromAddress );

        // then
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    public void retrieveStoreDelegatesToStoreCopyService() throws StoreCopyFailedException, StreamingTransactionsFailedException
    {
        // given
        StoreId storeId = new StoreId( 92, 5, 7, 32 );
        File anyFile = mock( File.class );

        // when
        subject.copy( anyAddress, storeId, anyFile );

        // then
        verify( remoteStore ).copy( anyAddress, storeId, anyFile );
    }

    @Test
    public void clearIdFilesDelegatesToClearIdService()
    {
        // given
        FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
        File file = new File( "any file name" );

        // when
        subject.clearIdFiles( fileSystemAbstraction, file );

        // then
        verify( clearIdService ).clearIdFiles( fileSystemAbstraction, file );
    }
}
