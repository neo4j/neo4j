/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.File;
import java.nio.file.Paths;
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
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.filesCopyAdaptor;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.prepareStoreCopyAdaptor;
import static org.neo4j.causalclustering.helper.RandomStringUtil.generateId;
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
    private final Monitors monitors;

    public StoreCopyClient( CatchUpClient catchUpClient, Monitors monitors, EventHandlerProvider eventHandlerProvider, TimeoutStrategy backOffStrategy )
    {
        this.catchUpClient = catchUpClient;
        this.monitors = monitors;
        this.eventHandlerProvider = eventHandlerProvider;
        this.backOffStrategy = backOffStrategy;
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreamProvider storeFileStreamProvider,
            Supplier<TerminationCondition> requestWiseTerminationCondition, File destDir )
            throws StoreCopyFailedException
    {
        EventHandler eventHandler = eventHandlerProvider.eventHandler( EventId.create() );
        eventHandler.on( Begin, "Copy store" );
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse =
                    prepareStoreCopy( catchupAddressProvider.primary(), expectedStoreId, storeFileStreamProvider, eventHandler );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider, requestWiseTerminationCondition,
                    eventHandler, destDir );
            copyIndexSnapshotIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition, eventHandler );
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
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, EventHandler eventHandler, File destDir )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            storeCopyClientMonitor.startReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
            persistentCallToSecondary( new GetStoreFileRequest( expectedStoreId, file, lastTransactionId, generateId() ),
                                       filesCopyAdaptor( storeFileStream, eventHandler ), addressProvider, terminationConditions.get(), eventHandler );
            storeCopyClientMonitor.finishReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
        }
        storeCopyClientMonitor.finishReceivingStoreFiles();
    }

    private void copyIndexSnapshotIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId,
            CatchupAddressProvider addressProvider, StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions,
            EventHandler eventHandler )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        LongIterator indexIds = prepareStoreCopyResponse.getIndexIds().longIterator();
        storeCopyClientMonitor.startReceivingIndexSnapshots();
        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            storeCopyClientMonitor.startReceivingIndexSnapshot( indexId );
            persistentCallToSecondary( new GetIndexFilesRequest( expectedStoreId, indexId, lastTransactionId, generateId() ),
                                       filesCopyAdaptor( storeFileStream, eventHandler ), addressProvider, terminationConditions.get(), eventHandler );
            storeCopyClientMonitor.finishReceivingIndexSnapshot( indexId );
        }
        storeCopyClientMonitor.finishReceivingIndexSnapshots();
    }

    private void persistentCallToSecondary( CatchUpRequest request, CatchUpResponseAdaptor<StoreCopyFinishedResponse> copyHandler,
            CatchupAddressProvider addressProvider, TerminationCondition terminationCondition, EventHandler eventHandler ) throws StoreCopyFailedException
    {
        TimeoutStrategy.Timeout timeout = backOffStrategy.newTimeout();
        boolean successful;
        do
        {
            try
            {
                AdvertisedSocketAddress address = addressProvider.secondary();
                eventHandler.on( Info, "Sending request", param( "RequestId", request.messageId() ), param( "Address", address ), param( "Request", request ) );
                StoreCopyFinishedResponse response = catchUpClient.makeBlockingRequest( address, request, copyHandler );
                successful = successfulRequest( response, request, eventHandler );
            }
            catch ( CatchUpClientException | CatchupAddressResolutionException e )
            {
                eventHandler.on( Warn, "Request failed", param( "Cause", e ) );
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
            EventHandler eventHandler )
            throws CatchUpClientException, StoreCopyFailedException
    {
        PrepareStoreCopyRequest request = new PrepareStoreCopyRequest( expectedStoreId, generateId() );
        eventHandler.on( Info, "Requesting store listing", param( "Address", from ), param( "RequestId", request.messageId() ) );
        PrepareStoreCopyResponse prepareStoreCopyResponse = catchUpClient.makeBlockingRequest( from, request,
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
            return catchUpClient.makeBlockingRequest( fromAddress, new GetStoreIdRequest( generateId() ), responseHandler );
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }

    private boolean successfulRequest( StoreCopyFinishedResponse response, CatchUpRequest request, EventHandler eventHandler ) throws StoreCopyFailedException
    {
        StoreCopyFinishedResponse.Status responseStatus = response.status();
        if ( responseStatus == StoreCopyFinishedResponse.Status.SUCCESS )
        {
            eventHandler.on( Info, "Request was successful" );
            return true;
        }
        else if ( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND == responseStatus || StoreCopyFinishedResponse.Status.E_UNKNOWN == responseStatus ||
                StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH == responseStatus )
        {
            eventHandler.on( Warn, "Request failed", param( "Response", response.status() ) );
            return false;
        }
        else
        {
            throw new StoreCopyFailedException( format( "Request responded with an unknown response type: %s. '%s'", responseStatus, request ) );
        }
    }
}
