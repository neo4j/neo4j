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
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.filesCopyAdaptor;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.prepareStoreCopyAdaptor;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Begin;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.End;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Error;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Info;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Warn;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class StoreCopyClient
{
    private final CatchUpClient catchUpClient;
    private final EventHandlerProvider eventHandlerProvider;
    private TimeoutStrategy backOffStrategy;

    public StoreCopyClient( CatchUpClient catchUpClient, EventHandlerProvider eventHandlerProvider, TimeoutStrategy backOffStrategy )
    {
        this.catchUpClient = catchUpClient;
        this.eventHandlerProvider = eventHandlerProvider;
        this.backOffStrategy = backOffStrategy;
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreamProvider storeFileStreamProvider,
            Supplier<TerminationCondition> requestWiseTerminationCondition )
            throws StoreCopyFailedException
    {
        EventId eventId = EventId.create();
        EventHandler eventHandler = eventHandlerProvider.eventHandler( eventId );
        eventHandler.on( Begin, "Copy store" );
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse =
                    prepareStoreCopy( catchupAddressProvider.primary(), expectedStoreId, storeFileStreamProvider, eventHandler, eventId.toString() );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider, requestWiseTerminationCondition,
                    eventHandler, eventId );
            copyIndexSnapshotIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition, eventHandler, eventId );
            return prepareStoreCopyResponse.lastTransactionId();
        }
        catch ( CatchupAddressResolutionException | CatchUpClientException e )
        {
            eventHandler.on( Error, "Copy store", e );
            throw new StoreCopyFailedException( e );
        }
        finally
        {
            eventHandler.on( End, "Copy store" );
        }
    }

    private void copyFilesIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, EventHandler eventHandler, EventId eventId )
            throws StoreCopyFailedException
    {
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            persistentCall( new GetStoreFileRequest( expectedStoreId, file, lastTransactionId, eventId.toString() ),
                    filesCopyAdaptor( storeFileStream, eventHandler ), addressProvider, terminationConditions.get(), eventHandler );
        }
    }

    private void copyIndexSnapshotIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId,
            CatchupAddressProvider addressProvider, StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions,
            EventHandler eventHandler, EventId eventId ) throws StoreCopyFailedException
    {
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        PrimitiveLongIterator indexIds = prepareStoreCopyResponse.getIndexIds().iterator();
        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            persistentCall( new GetIndexFilesRequest( expectedStoreId, indexId, lastTransactionId, eventId.toString() ),
                    filesCopyAdaptor( storeFileStream, eventHandler ), addressProvider, terminationConditions.get(), eventHandler );
        }
    }

    private void persistentCall( CatchUpRequest request, CatchUpResponseAdaptor<StoreCopyFinishedResponse> copyHandler, CatchupAddressProvider addressProvider,
            TerminationCondition terminationCondition, EventHandler eventHandler ) throws StoreCopyFailedException
    {
        TimeoutStrategy.Timeout timeout = backOffStrategy.newTimeout();
        boolean successful;
        do
        {
            try
            {
                AdvertisedSocketAddress from = addressProvider.secondary();
                eventHandler.on( Info, "Sending request", param( "Request", request ), param( "Address", from ) );
                StoreCopyFinishedResponse response = catchUpClient.makeBlockingRequest( from, request, copyHandler );
                successful = successfulFileDownload( response );
                if ( !successful )
                {
                    eventHandler.on( Warn, "Request failed", param( "Status", response.status() ) );
                }
                else
                {
                    eventHandler.on( Info, "Request succeeded" );
                }
            }
            catch ( CatchUpClientException | CatchupAddressResolutionException e )
            {
                eventHandler.on( Warn, "Request failed", e );
                successful = false;
            }
            if ( !successful )
            {
                terminationCondition.assertContinue();
            }
            awaitAndIncrementTimeout( timeout );
        }
        while ( !successful );
    }

    private void awaitAndIncrementTimeout( TimeoutStrategy.Timeout timeout ) throws StoreCopyFailedException
    {
        try
        {
            Thread.sleep( timeout.getMillis() );
            timeout.increment();
        }
        catch ( InterruptedException e )
        {
            throw new StoreCopyFailedException( "Thread interrupted" );
        }
    }

    private PrepareStoreCopyResponse prepareStoreCopy( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream,
            EventHandler eventHandler, String eventId )
            throws CatchUpClientException, StoreCopyFailedException
    {
        eventHandler.on( Info, "Requesting store listing", param( "Address", from ) );
        PrepareStoreCopyResponse prepareStoreCopyResponse = catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId, eventId ),
                prepareStoreCopyAdaptor( storeFileStream, eventHandler ) );
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
            return catchUpClient.makeBlockingRequest( fromAddress, new GetStoreIdRequest( EventId.create().toString() ), responseHandler );
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }

    private boolean successfulFileDownload( StoreCopyFinishedResponse response ) throws StoreCopyFailedException
    {
        StoreCopyFinishedResponse.Status responseStatus = response.status();
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
