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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreCopyClientTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );

    private StoreCopyClient subject;
    private final LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private final Monitors monitors = new Monitors();

    // params
    private final AdvertisedSocketAddress expectedAdvertisedAddress = new AdvertisedSocketAddress( "host", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( expectedAdvertisedAddress );
    private final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
    private final StoreFileStreamProvider expectedStoreFileStream = mock( StoreFileStreamProvider.class );

    // helpers
    private File[] serverFiles = new File[]{new File( "fileA.txt" ), new File( "fileB.bmp" )};
    private File targetLocation = new File( "targetLocation" );
    private PrimitiveLongSet indexIds = Primitive.longSet();
    private ConstantTimeTimeoutStrategy backOffStrategy;

    @Before
    public void setup()
    {
        indexIds.add( 13 );
        backOffStrategy = new ConstantTimeTimeoutStrategy( 1, TimeUnit.MILLISECONDS );
        subject = new StoreCopyClient( catchUpClient, monitors, logProvider, backOffStrategy );
    }

    @Test
    public void clientRequestsAllFilesListedInListingResponse() throws StoreCopyFailedException, CatchUpClientException
    {
        // given a bunch of fake files on the server
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and any request for a file will be successful
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and any request for a file will be successful
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // when client requests catchup
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then there are as many requests to the server for individual requests
        List<String> filteredRequests = filenamesFromIndividualFileRequests( getRequests() );
        List<String> expectedFiles = Stream.of( serverFiles ).map( File::getName ).collect( Collectors.toList() );
        assertThat( expectedFiles, containsInAnyOrder( filteredRequests.toArray() ) );
    }

    private Supplier<TerminationCondition> continueIndefinitely()
    {
        return () -> TerminationCondition.CONTINUE_INDEFINITELY;
    }

    @Test
    public void storeIdCanBeRetrieved() throws StoreIdDownloadFailedException, CatchUpClientException
    {
        // given remote has expected store ID
        StoreId remoteStoreId = new StoreId( 6, 3, 2, 6 );

        // and we know the remote address
        AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "host", 1234 );

        // and server responds with correct data to correct params
        when( catchUpClient.makeBlockingRequest( eq( remoteAddress ), any( GetStoreIdRequest.class ), any() ) ).thenReturn( remoteStoreId );

        // when client requests the remote store id
        StoreId actualStoreId = subject.fetchStoreId( remoteAddress );

        // then store id matches
        assertEquals( remoteStoreId, actualStoreId );
    }

    @Test
    public void shouldNotAwaitOnSuccess() throws CatchUpClientException, StoreCopyFailedException
    {
        // given
        TimeoutStrategy.Timeout mockedTimeout = mock( TimeoutStrategy.Timeout.class );
        TimeoutStrategy backoffStrategy = mock( TimeoutStrategy.class );
        when( backoffStrategy.newTimeout() ).thenReturn( mockedTimeout );

        // and
        subject = new StoreCopyClient( catchUpClient, monitors, logProvider, backoffStrategy );

        // and
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( mockedTimeout, never() ).increment();
        verify( mockedTimeout, never() ).getMillis();
    }

    @Test
    public void shouldFailIfTerminationConditionFails() throws CatchUpClientException
    {
        // given a file will fail an expected number of times
        subject = new StoreCopyClient( catchUpClient, monitors, logProvider, backOffStrategy );

        // and requesting the individual file will fail
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn(
                new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND ) );

        // and the initial list+count store files request is successful
        PrepareStoreCopyResponse initialListingOfFilesResponse = PrepareStoreCopyResponse.success( serverFiles,
                indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( initialListingOfFilesResponse );

        // when we perform catchup
        try
        {
            subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, () -> () ->
            {
                throw new StoreCopyFailedException( "This can't go on" );
            }, targetLocation );
            fail( "Expected exception: " + StoreCopyFailedException.class );
        }
        catch ( StoreCopyFailedException expectedException )
        {
            assertEquals( "This can't go on", expectedException.getMessage() );
            return;
        }

        fail( "Expected a StoreCopyFailedException" );
    }

    @Test
    public void errorOnListingStore() throws CatchUpClientException, StoreCopyFailedException
    {
        // given store listing fails
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn( prepareStoreCopyResponse )
                .thenThrow( new RuntimeException( "Should not be accessible" ) );

        // then
        expectedException.expectMessage( "Preparing store failed due to: E_LISTING_STORE" );
        expectedException.expect( StoreCopyFailedException.class );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );
    }

    @Test
    public void storeIdMismatchOnListing() throws CatchUpClientException, StoreCopyFailedException
    {
        // given store listing fails
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn( prepareStoreCopyResponse )
                .thenThrow( new RuntimeException( "Should not be accessible" ) );

        // then
        expectedException.expectMessage( "Preparing store failed due to: E_STORE_ID_MISMATCH" );
        expectedException.expect( StoreCopyFailedException.class );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );
    }

    @Test
    public void storeFileEventsAreReported() throws Exception
    {
        // given
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // and
        StoreCopyClientMonitor storeCopyClientMonitor = mock( StoreCopyClientMonitor.class );
        monitors.addMonitorListener( storeCopyClientMonitor );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( storeCopyClientMonitor ).startReceivingStoreFiles();
        for ( File storeFileRequested : serverFiles )
        {
            verify( storeCopyClientMonitor ).startReceivingStoreFile( Paths.get( targetLocation.toString(), storeFileRequested.toString() ).toString() );
            verify( storeCopyClientMonitor ).finishReceivingStoreFile( Paths.get( targetLocation.toString(), storeFileRequested.toString() ).toString() );
        }
        verify( storeCopyClientMonitor ).finishReceivingStoreFiles();
    }

    @Test
    public void snapshotEventsAreReported() throws Exception
    {
        // given
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // and
        StoreCopyClientMonitor storeCopyClientMonitor = mock( StoreCopyClientMonitor.class );
        monitors.addMonitorListener( storeCopyClientMonitor );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( storeCopyClientMonitor ).startReceivingIndexSnapshots();
        PrimitiveLongIterator iterator = indexIds.iterator();
        while ( iterator.hasNext() )
        {
            long indexSnapshotIdRequested = iterator.next();
            verify( storeCopyClientMonitor ).startReceivingIndexSnapshot( indexSnapshotIdRequested );
            verify( storeCopyClientMonitor ).finishReceivingIndexSnapshot( indexSnapshotIdRequested );
        }
        verify( storeCopyClientMonitor ).finishReceivingIndexSnapshots();
    }

    private List<CatchUpRequest> getRequests() throws CatchUpClientException
    {
        ArgumentCaptor<CatchUpRequest> fileRequestArgumentCaptor = ArgumentCaptor.forClass( CatchUpRequest.class );
        verify( catchUpClient, atLeast( 0 ) ).makeBlockingRequest( any(), fileRequestArgumentCaptor.capture(), any() );
        return fileRequestArgumentCaptor.getAllValues();
    }

    private List<String> filenamesFromIndividualFileRequests( List<CatchUpRequest> fileRequests )
    {
        return fileRequests.stream()
                .filter( GetStoreFileRequest.class::isInstance )
                .map( obj -> (GetStoreFileRequest) obj )
                .map( GetStoreFileRequest::file )
                .map( File::getName )
                .collect( Collectors.toList() );
    }
}

