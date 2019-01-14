/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupDelegatorTest
{
    private RemoteStore remoteStore;
    private CatchUpClient catchUpClient;
    private StoreCopyClient storeCopyClient;

    BackupDelegator subject;

    private final AdvertisedSocketAddress anyAddress = new AdvertisedSocketAddress( "any.address", 1234 );

    @Before
    public void setup()
    {
        remoteStore = mock( RemoteStore.class );
        catchUpClient = mock( CatchUpClient.class );
        storeCopyClient = mock( StoreCopyClient.class );
        subject = new BackupDelegator( remoteStore, catchUpClient, storeCopyClient );
    }

    @Test
    public void tryCatchingUpDelegatesToRemoteStore() throws StoreCopyFailedException, IOException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4j.com", 5432 );
        StoreId expectedStoreId = new StoreId( 7, 2, 5, 98 );
        Path storeDir = Paths.get( "A directory to store transactions to" );

        // when
        subject.tryCatchingUp( fromAddress, expectedStoreId, storeDir );

        // then
        verify( remoteStore ).tryCatchingUp( fromAddress, expectedStoreId, storeDir.toFile(), true, true );
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
        when( storeCopyClient.fetchStoreId( fromAddress ) ).thenReturn( expectedStoreId );

        // when
        StoreId storeId = subject.fetchStoreId( fromAddress );

        // then
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    public void retrieveStoreDelegatesToStoreCopyService()
            throws StoreCopyFailedException, CatchupAddressResolutionException
    {
        // given
        StoreId storeId = new StoreId( 92, 5, 7, 32 );
        Path anyFile = Paths.get( "anywhere" );

        // when
        subject.copy( anyAddress, storeId, anyFile );

        // then
        ArgumentCaptor<CatchupAddressProvider> argumentCaptor = ArgumentCaptor.forClass( CatchupAddressProvider.class );
        verify( remoteStore ).copy( argumentCaptor.capture(), eq( storeId ), eq( anyFile.toFile() ), eq( true ) );

        //and
        assertEquals( anyAddress, argumentCaptor.getValue().primary() );
    }
}
