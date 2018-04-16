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
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.filesCopyAdaptor;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.prepareStoreCopyAdaptor;

public class StoreCopyClient
{
    private final CatchUpClient catchUpClient;
    private final Log log;
    private TimeoutStrategy backOffStrategy;
    private final Monitors monitors;

    public StoreCopyClient( CatchUpClient catchUpClient, Monitors monitors, LogProvider logProvider, TimeoutStrategy backOffStrategy )
    {
        this.catchUpClient = catchUpClient;
        this.monitors = monitors;
        log = logProvider.getLog( getClass() );
        this.backOffStrategy = backOffStrategy;
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreamProvider storeFileStreamProvider,
            Supplier<TerminationCondition> requestWiseTerminationCondition, File destDir )
            throws StoreCopyFailedException
    {
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = prepareStoreCopy( catchupAddressProvider.primary(), expectedStoreId, storeFileStreamProvider );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition, destDir );
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
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, File destDir ) throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            storeCopyClientMonitor.startReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
            persistentCallToSecondary( new GetStoreFileRequest( expectedStoreId, file, lastTransactionId ), filesCopyAdaptor( storeFileStream, log ),
                    addressProvider,
                    terminationConditions.get() );
            storeCopyClientMonitor.finishReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
        }
        storeCopyClientMonitor.finishReceivingStoreFiles();
    }

    private void copyIndexSnapshotIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId,
            CatchupAddressProvider addressProvider, StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        PrimitiveLongIterator indexIds = prepareStoreCopyResponse.getIndexIds().iterator();
        storeCopyClientMonitor.startReceivingIndexSnapshots();
        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            storeCopyClientMonitor.startReceivingIndexSnapshot( indexId );
            persistentCallToSecondary( new GetIndexFilesRequest( expectedStoreId, indexId, lastTransactionId ), filesCopyAdaptor( storeFileStream, log ),
                    addressProvider,
                    terminationConditions.get() );
            storeCopyClientMonitor.finishReceivingIndexSnapshot( indexId );
        }
        storeCopyClientMonitor.finishReceivingIndexSnapshots();
    }

    private void persistentCallToSecondary( CatchUpRequest request, CatchUpResponseAdaptor<StoreCopyFinishedResponse> copyHandler,
            CatchupAddressProvider addressProvider,
            TerminationCondition terminationCondition ) throws StoreCopyFailedException
    {
        TimeoutStrategy.Timeout timeout = backOffStrategy.newTimeout();
        boolean successful;
        do
        {
            try
            {
                AdvertisedSocketAddress address = addressProvider.secondary();
                log.info( format( "Sending request '%s' to '%s'", request, address ) );
                StoreCopyFinishedResponse response = catchUpClient.makeBlockingRequest( address, request, copyHandler );
                successful = successfulRequest( response, request );
            }
            catch ( CatchUpClientException | CatchupAddressResolutionException e )
            {
                log.warn( format( "Request failed exceptionally '%s'.", request ), e );
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

    private PrepareStoreCopyResponse prepareStoreCopy( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream )
            throws CatchUpClientException, StoreCopyFailedException
    {
        log.info( "Requesting store listing from: " + from );
        PrepareStoreCopyResponse prepareStoreCopyResponse =
                catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId ), prepareStoreCopyAdaptor( storeFileStream, log ) );
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

    private boolean successfulRequest( StoreCopyFinishedResponse response, CatchUpRequest request ) throws StoreCopyFailedException
    {
        StoreCopyFinishedResponse.Status responseStatus = response.status();
        if ( responseStatus == StoreCopyFinishedResponse.Status.SUCCESS )
        {
            log.info( format( "Request was successful '%s'", request ) );
            return true;
        }
        else if ( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND == responseStatus || StoreCopyFinishedResponse.Status.E_UNKNOWN == responseStatus ||
                StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH == responseStatus )
        {
            log.warn( format( "Request failed '%s'. With response: %s", request, response.status() ) );
            return false;
        }
        else
        {
            throw new StoreCopyFailedException( format( "Request responded with an unknown response type: %s. '%s'", responseStatus, request ) );
        }
    }
}
