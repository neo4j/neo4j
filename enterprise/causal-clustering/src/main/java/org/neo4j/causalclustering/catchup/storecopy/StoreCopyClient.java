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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class StoreCopyClient
{
    private final CatchUpClient catchUpClient;
    private final Log log;

    public StoreCopyClient( CatchUpClient catchUpClient, LogProvider logProvider )
    {
        this.catchUpClient = catchUpClient;
        log = logProvider.getLog( getClass() );
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreamProvider storeFileStreamProvider,
            Supplier<TerminationCondition> requestWiseTerminationCondition )
            throws StoreCopyFailedException
    {
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = prepareStoreCopy( catchupAddressProvider.primary(), expectedStoreId, storeFileStreamProvider );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition );
            copyIndexSnapshotIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition );
            return prepareStoreCopyResponse.lastTransactionId();
        }
        catch ( CatchupAddressResolutionException | CatchUpClientException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private void copyFilesIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions ) throws StoreCopyFailedException
    {
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            TerminationCondition terminationCondition = terminationConditions.get();
            boolean successful;
            do
            {
                try
                {
                    AdvertisedSocketAddress from = addressProvider.primary();
                    log.info( format( "Downloading file '%s' from '%s'", file, from ) );
                    StoreCopyFinishedResponse response =
                            catchUpClient.makeBlockingRequest( from, new GetStoreFileRequest( expectedStoreId, file, lastTransactionId ),
                                    StoreCopyResponseAdaptors.filesCopyAdaptor( storeFileStream, log ) );
                    successful = successfulFileDownload( response );
                }
                catch ( CatchUpClientException | CatchupAddressResolutionException e )
                {
                    successful = false;
                }
                if ( !successful )
                {
                    log.error( "Failed to download file '%s'", file );
                    terminationCondition.assertContinue();
                }
            }
            while ( !successful );
        }
    }

    private void copyIndexSnapshotIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId,
            CatchupAddressProvider addressProvider, StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions )
            throws StoreCopyFailedException
    {
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        PrimitiveLongIterator indexIds = prepareStoreCopyResponse.getIndexIds().iterator();
        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            TerminationCondition terminationCondition = terminationConditions.get();
            boolean successful;
            do
            {
                try
                {
                    AdvertisedSocketAddress from = addressProvider.primary();
                    log.info( format( "Downloading snapshot of index '%s' from '%s'", indexId, from ) );
                    StoreCopyFinishedResponse response =
                            catchUpClient.makeBlockingRequest( from, new GetIndexFilesRequest( expectedStoreId, indexId, lastTransactionId ),
                                    StoreCopyResponseAdaptors.filesCopyAdaptor( storeFileStream, log ) );
                    successful = successfulFileDownload( response );
                }
                catch ( CatchUpClientException | CatchupAddressResolutionException e )
                {
                    successful = false;
                }
                if ( !successful )
                {
                    log.error( "Failed to download files from index '%s'", indexId );
                    terminationCondition.assertContinue();
                }
            }
            while ( !successful );
        }
    }

    private PrepareStoreCopyResponse prepareStoreCopy( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream )
            throws CatchUpClientException, StoreCopyFailedException
    {
        log.info( "Requesting store listing from: " + from );
        PrepareStoreCopyResponse prepareStoreCopyResponse = catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId ),
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( storeFileStream, log ) );
        if ( prepareStoreCopyResponse.status() != PrepareStoreCopyResponse.Status.SUCCESS )
        {
            throw new StoreCopyFailedException( "Preparing store failed due to: " + prepareStoreCopyResponse.status() );
        }
        return prepareStoreCopyResponse;
    }

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        try
        {
            CatchUpResponseAdaptor<StoreId> responseHandler = new CatchUpResponseAdaptor<StoreId>()
            {
                @Override
                public void onGetStoreIdResponse( CompletableFuture<StoreId> signal, GetStoreIdResponse response )
                {
                    signal.complete( response.storeId() );
                }
            };
            return catchUpClient.makeBlockingRequest( fromAddress, new GetStoreIdRequest(), responseHandler );
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }

    private boolean successfulFileDownload( StoreCopyFinishedResponse response ) throws StoreCopyFailedException
    {
        StoreCopyFinishedResponse.Status responseStatus = response.status();
        log.debug( "Request for individual file resulted in response type: %s", response.status() );
        if ( responseStatus == StoreCopyFinishedResponse.Status.SUCCESS )
        {
            return true;
        }
        else if ( responseStatus == StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND )
        {
            return false;
        }
        else if ( responseStatus == StoreCopyFinishedResponse.Status.E_UNKNOWN )
        {
            return false;
        }
        else if ( responseStatus == StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH )
        {
            return false;
        }
        else
        {
            throw new StoreCopyFailedException( "Unknown response type: " + responseStatus );
        }
    }
}
