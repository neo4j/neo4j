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
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class StoreCopyClient
{
    private final CatchUpClient catchUpClient;
    private final Log log;
    private final LogProvider logProvider;

    public StoreCopyClient( CatchUpClient catchUpClient, LogProvider logProvider )
    {
        this.catchUpClient = catchUpClient;
        log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreams storeFileStreams,
            Supplier<TerminationCondition> requestWiseTerminationCondition )
            throws StoreCopyFailedException
    {
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = listFiles( catchupAddressProvider.primary(), expectedStoreId, storeFileStreams );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreams, requestWiseTerminationCondition );
            copyIndexSnapshotIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreams,
                    requestWiseTerminationCondition );
            return prepareStoreCopyResponse.lastTransactionId();
        }
        catch ( CatchupAddressResolutionException | CatchUpClientException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private void copyFilesIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
            StoreFileStreams storeFileStreams, Supplier<TerminationCondition> terminationConditions ) throws StoreCopyFailedException
    {
        CatchUpResponseAdaptor<StoreCopyFinishedResponse> copyHandler = new StoreFileCopyResponseAdaptor( storeFileStreams, log );
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
                    log.info( String.format( "Downloading file '%s' from '%s'", file, from ) );
                    StoreCopyFinishedResponse response =
                            catchUpClient.makeBlockingRequest( from, new GetStoreFileRequest( expectedStoreId, file, lastTransactionId ), copyHandler );
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
            CatchupAddressProvider addressProvider,
            StoreFileStreams storeFileStreams, Supplier<TerminationCondition> terminationConditions ) throws StoreCopyFailedException
    {
        CatchUpResponseAdaptor<StoreCopyFinishedResponse> copyHandler = new StoreFileCopyResponseAdaptor( storeFileStreams, log );
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        for ( IndexDescriptor descriptor : prepareStoreCopyResponse.getDescriptors() )
        {
            TerminationCondition terminationCondition = terminationConditions.get();
            boolean successful;
            do
            {
                try
                {
                    AdvertisedSocketAddress from = addressProvider.primary();
                    log.info( String.format( "Downloading snapshot '%s' from '%s'", descriptor, from ) );
                    StoreCopyFinishedResponse response =
                            catchUpClient.makeBlockingRequest( from, new GetIndexFilesRequest( expectedStoreId, descriptor, lastTransactionId ),
                                    copyHandler );
                    successful = successfulFileDownload( response );
                }
                catch ( CatchUpClientException | CatchupAddressResolutionException e )
                {
                    successful = false;
                }
                if ( !successful )
                {
                    log.error( "Failed to download file '%s'", descriptor );
                    terminationCondition.assertContinue();
                }
            }
            while ( !successful );
        }
    }

    private PrepareStoreCopyResponse listFiles( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreams storeFileStreams )
            throws CatchUpClientException, StoreCopyFailedException
    {
        log.info( "Requesting store listing from: " + from );
        PrepareStoreCopyResponse prepareStoreCopyResponse = catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId ),
                new PrepareStoreCopyResponseAdaptor( storeFileStreams, logProvider ) );
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
        if ( responseStatus == StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND )
        {
            return false;
        }
        else if ( responseStatus == StoreCopyFinishedResponse.Status.SUCCESS )
        {
            return true;
        }
        else if ( responseStatus == StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH )
        {
            throw new StoreCopyFailedException( "Store id mismatch" );
        }
        else
        {
            throw new StoreCopyFailedException( "Unknown response type: " + responseStatus );
        }
    }

    public static class StoreFileCopyResponseAdaptor extends CatchUpResponseAdaptor<StoreCopyFinishedResponse>
    {
        private final StoreFileStreams storeFileStreams;
        private final Log log;
        private String destination;
        private int requiredAlignment;

        StoreFileCopyResponseAdaptor( StoreFileStreams storeFileStreams, Log log )
        {
            this.storeFileStreams = storeFileStreams;
            this.log = log;
        }

        @Override
        public void onFileHeader( CompletableFuture<StoreCopyFinishedResponse> requestOutcomeSignal, FileHeader fileHeader )
        {
            this.destination = fileHeader.fileName();
            this.requiredAlignment = fileHeader.requiredAlignment();
        }

        @Override
        public boolean onFileContent( CompletableFuture<StoreCopyFinishedResponse> signal, FileChunk fileChunk ) throws IOException
        {
            storeFileStreams.write( destination, requiredAlignment, fileChunk.bytes() );
            return fileChunk.isLast();
        }

        @Override
        public void onFileStreamingComplete( CompletableFuture<StoreCopyFinishedResponse> signal, StoreCopyFinishedResponse response )
        {
            log.info( "Finished streaming" );
            signal.complete( response );
        }
    }
}
