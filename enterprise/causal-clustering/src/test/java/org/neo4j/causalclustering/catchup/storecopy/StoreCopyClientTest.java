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
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.causalclustering.messaging.CompositeEventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.LoggingEventHandlerProvider;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
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
    private final CompositeEventHandlerProvider eventHandler = CompositeEventHandlerProvider.merge( new LoggingEventHandlerProvider(
            FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ).getLog( StoreCopyClient.class ) ) );

    // params
    private final AdvertisedSocketAddress expectedAdvertisedAddress = new AdvertisedSocketAddress( "host", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( expectedAdvertisedAddress );
    private final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
    private final StoreFileStreamProvider expectedStoreFileStream = mock( StoreFileStreamProvider.class );

    // helpers
    private File[] serverFiles = new File[]{new File( "fileA.txt" ), new File( "fileB.bmp" )};
    private File targetLocation = new File( "targetLocation" );
    private LongSet indexIds = LongSets.immutable.of( 13 );
    private ConstantTimeTimeoutStrategy backOffStrategy;

    @Before
    public void setup()
    {
        backOffStrategy = new ConstantTimeTimeoutStrategy( 1, TimeUnit.MILLISECONDS );
        subject = new StoreCopyClient( catchUpClient, eventHandler, backOffStrategy );
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
    public void shouldFailIfTerminationConditionFails() throws CatchUpClientException
    {
        // given a file will fail an expected number of times
        subject = new StoreCopyClient( catchUpClient, eventHandler, backOffStrategy );

        // and requesting the individual file will fail
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn(
                new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND ) );

        // and the initial list+count store files request is successful
        PrepareStoreCopyResponse initialListingOfFilesResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
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
    public void shouldFollowExpectedEventPattern() throws Exception
    {
        // given
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and
        StateCountingEventHandler stateCountingEventHandler = new StateCountingEventHandler();
        eventHandler.add( id -> stateCountingEventHandler );

        // and
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        Map<EventHandler.EventState,Integer> states = stateCountingEventHandler.states;
        assertThat( states.get( EventHandler.EventState.Begin ), equalTo( 1 ) );
        assertTrue( states.get( EventHandler.EventState.Info ) > 1 );
        assertThat( states.get( EventHandler.EventState.End ), equalTo( 1 ) );
    }

    private class StateCountingEventHandler implements EventHandler
    {
        final Map<EventState,Integer> states = new HashMap<>();

        StateCountingEventHandler()
        {
            for ( EventState eventState : EventState.values() )
            {
                states.putIfAbsent( eventState, 0 );
            }
        }

        @Override
        public void on( EventState eventState, String message, Throwable throwable, Param... params )
        {
            states.put( eventState, states.get( eventState ) + 1 );
        }
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

