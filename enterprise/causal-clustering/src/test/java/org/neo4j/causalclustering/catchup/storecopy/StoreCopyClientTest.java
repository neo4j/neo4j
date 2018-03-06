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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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

    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );

    private StoreCopyClient subject;
    private final LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    // params
    private final AdvertisedSocketAddress expectedAdvertisedAddress = new AdvertisedSocketAddress( "host", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( expectedAdvertisedAddress );
    private final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
    private final StoreFileStreams expectedStoreFileStreams = mock( StoreFileStreams.class );

    // helpers
    private File[] serverFiles = new File[]{new File( "fileA.txt" ), new File( "fileB.bmp" )};
    private IndexDescriptor[] descriptors = new IndexDescriptor[]{new IndexDescriptor( new LabelSchemaDescriptor( 1, 2, 3 ), IndexDescriptor.Type.GENERAL )};

    @Before
    public void setup()
    {
        subject = new StoreCopyClient( catchUpClient, logProvider );
    }

    @Test
    public void clientRequestsAllFilesListedInListingResponse() throws StoreCopyFailedException, CatchUpClientException
    {
        // given a bunch of fake files on the server
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, descriptors, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and any request for a file will be successful
        StoreCopyFinishedResponse success = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS );
        when( catchUpClient.makeBlockingRequest( any(), any( GetStoreFileRequest.class ), any() ) ).thenReturn( success );

        // and any request for a file will be successful
        when( catchUpClient.makeBlockingRequest( any(), any( GetIndexFilesRequest.class ), any() ) ).thenReturn( success );

        // when client requests catchup
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStreams, continueIndefinitely() );

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
        subject = new StoreCopyClient( catchUpClient, logProvider );

        // and requesting the individual file will fail
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn(
                new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND ) );

        // and the initial list+count store files request is successful
        PrepareStoreCopyResponse initialListingOfFilesResponse = PrepareStoreCopyResponse.success( serverFiles, descriptors, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any( PrepareStoreCopyRequest.class ), any() ) ).thenReturn( initialListingOfFilesResponse );

        // when we perform catchup
        try
        {
            subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStreams, () -> () ->
            {
                throw new StoreCopyFailedException( "This can't go on" );
            } );
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
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStreams, continueIndefinitely() );
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
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStreams, continueIndefinitely() );
    }

    @Test
    public void storeIdMismatchOnCopyIndividualFile() throws StoreCopyFailedException, CatchUpClientException
    {
        // given listing response will be successful
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, descriptors, -123L );
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn( prepareStoreCopyResponse );

        // and individual file requests get store id mismatch
        StoreCopyFinishedResponse individualFileStoreCopyResposne = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH );
        when( catchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenReturn( prepareStoreCopyResponse, individualFileStoreCopyResposne );

        // then exception denotes store id mismatch
        expectedException.expect( StoreCopyFailedException.class );
        expectedException.expectMessage( "Store id mismatch" );

        // when copy is performed
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStreams, continueIndefinitely() );
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

