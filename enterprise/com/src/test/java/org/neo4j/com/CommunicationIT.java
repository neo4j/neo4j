/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.ports.allocation.PortAuthority;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.com.MadeUpServer.FRAME_LENGTH;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;
import static org.neo4j.com.TxChecksumVerifier.ALWAYS_MATCH;
import static org.neo4j.com.storecopy.ResponseUnpacker.TxHandler.NO_OP_TX_HANDLER;

public class CommunicationIT
{
    private static final byte INTERNAL_PROTOCOL_VERSION = 0;
    private static final byte APPLICATION_PROTOCOL_VERSION = 0;

    private final LifeSupport life = new LifeSupport();
    private StoreId storeIdToUse;
    private Builder builder;

    @Before
    public void doBefore()
    {
        storeIdToUse = newStoreIdForCurrentVersion();
        builder = new Builder();
    }

    @After
    public void shutdownLife()
    {
        life.shutdown();
    }

    @Test
    public void clientGetResponseFromServerViaComLayer() throws Throwable
    {
        MadeUpServerImplementation serverImplementation = new MadeUpServerImplementation( storeIdToUse );
        MadeUpServer server = builder.server( serverImplementation );
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        int value1 = 10;
        int value2 = 5;
        Response<Integer> response = client.multiply( 10, 5 );
        waitUntilResponseHasBeenWritten( server, 1000 );
        assertEquals( (Integer) (value1 * value2), response.response() );
        assertTrue( serverImplementation.gotCalled() );
        assertTrue( server.responseHasBeenWritten() );
    }

    private void waitUntilResponseHasBeenWritten( MadeUpServer server, int maxTime ) throws Exception
    {
        long time = currentTimeMillis();
        while ( !server.responseHasBeenWritten() && currentTimeMillis() - time < maxTime )
        {
            Thread.sleep( 50 );
        }
    }

    @Test( expected = MismatchingStoreIdException.class )
    public void makeSureClientStoreIdsMustMatch()
    {
        MadeUpServer server = builder.server();
        MadeUpClient client = builder.storeId( newStoreIdForCurrentVersion( 10, 10, 10, 10 ) ).client();
        addToLifeAndStart( server, client );

        client.multiply( 1, 2 );
    }

    @Test( expected = MismatchingStoreIdException.class )
    public void makeSureServerStoreIdsMustMatch()
    {
        MadeUpServer server = builder.storeId( newStoreIdForCurrentVersion( 10, 10, 10, 10 ) ).server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        client.multiply( 1, 2 );
    }

    @Test
    public void makeSureClientCanStreamBigData()
    {
        MadeUpServer server = builder.server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        client.fetchDataStream( new ToAssertionWriter(), FRAME_LENGTH * 3 );
    }

    @Test
    public void clientThrowsServerSideErrorMidwayThroughStreaming()
    {
        final String failureMessage = "Just failing";
        MadeUpServerImplementation serverImplementation = new MadeUpServerImplementation( storeIdToUse )
        {
            @Override
            public Response<Void> fetchDataStream( MadeUpWriter writer, int dataSize )
            {
                writer.write( new FailingByteChannel( dataSize, failureMessage ) );
                return new TransactionStreamResponse<>( null, storeIdToUse, TransactionStream.EMPTY,
                        ResourceReleaser.NO_OP );
            }
        };
        MadeUpServer server = builder.server( serverImplementation );
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        try
        {
            client.fetchDataStream( new ToAssertionWriter(), FRAME_LENGTH * 2 );
            fail( "Should have thrown " + MadeUpException.class.getSimpleName() );
        }
        catch ( MadeUpException e )
        {
            assertEquals( failureMessage, e.getMessage() );
        }
    }

    @Test
    public void communicateBetweenJvms()
    {
        ServerInterface server = builder.serverInOtherJvm();
        server.awaitStarted();
        MadeUpClient client = builder.port( MadeUpServerProcess.PORT ).client();
        life.add( client );
        life.start();

        assertEquals( (Integer) (9 * 5), client.multiply( 9, 5 ).response() );
        client.fetchDataStream( new ToAssertionWriter(), 1024 * 1024 * 3 );

        server.shutdown();
    }

    @Test
    public void throwingServerSideExceptionBackToClient()
    {
        MadeUpServer server = builder.server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        String exceptionMessage = "The message";
        try
        {
            client.throwException( exceptionMessage );
            fail( "Should have thrown " + MadeUpException.class.getSimpleName() );
        }
        catch ( MadeUpException e )
        {   // Good
            assertEquals( exceptionMessage, e.getMessage() );
        }
    }

    @Test
    public void applicationProtocolVersionsMustMatch()
    {
        MadeUpServer server = builder.applicationProtocolVersion( (byte) (APPLICATION_PROTOCOL_VERSION + 1) ).server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e )
        { /* Good */ }
    }

    @Test
    public void applicationProtocolVersionsMustMatchMultiJvm()
    {
        ServerInterface server = builder.applicationProtocolVersion( (byte) (APPLICATION_PROTOCOL_VERSION + 1) )
                                        .serverInOtherJvm();
        server.awaitStarted();
        MadeUpClient client = builder.port( MadeUpServerProcess.PORT ).client();
        life.add( client );
        life.start();

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e )
        { /* Good */ }

        server.shutdown();
    }

    @Test
    public void internalProtocolVersionsMustMatch()
    {
        MadeUpServer server = builder.internalProtocolVersion( (byte) 1 ).server();
        MadeUpClient client = builder.internalProtocolVersion( (byte) 2 ).client();
        addToLifeAndStart( server, client );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e )
        { /* Good */ }
    }

    @Test
    public void internalProtocolVersionsMustMatchMultiJvm()
    {
        ServerInterface server = builder.internalProtocolVersion( (byte) 1 ).serverInOtherJvm();
        server.awaitStarted();
        MadeUpClient client = builder.port( MadeUpServerProcess.PORT ).internalProtocolVersion( (byte) 2 ).client();
        life.add( client );
        life.start();

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e )
        { /* Good */ }

        server.shutdown();
    }

    @Test
    public void serverStopsStreamingToDeadClient() throws Throwable
    {
        MadeUpServer server = builder.server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        int failAtSize = FRAME_LENGTH / 1024;
        ClientCrashingWriter writer = new ClientCrashingWriter( client, failAtSize );
        try
        {
            client.fetchDataStream( writer, FRAME_LENGTH * 100 );
            assertTrue( writer.getSizeRead() >= failAtSize );
            fail( "Should fail in the middle" );
        }
        catch ( ComException e )
        {   // Expected
        }
        assertTrue( writer.getSizeRead() >= failAtSize );

        long maxWaitUntil = System.currentTimeMillis() + 60_000L;
        while ( !server.responseFailureEncountered() && System.currentTimeMillis() < maxWaitUntil )
        {
            sleep( 100 );
        }
        assertTrue( "Failure writing the response should have been encountered", server.responseFailureEncountered() );
        assertFalse( "Response shouldn't have been successful", server.responseHasBeenWritten() );
    }

    @Test
    public void serverContextVerificationCanThrowException()
    {
        final String failureMessage = "I'm failing";
        TxChecksumVerifier failingVerifier = ( txId, checksum ) ->
        {
            throw new FailingException( failureMessage );
        };

        MadeUpServer server = builder.verifier( failingVerifier ).server();
        MadeUpClient client = builder.client();
        addToLifeAndStart( server, client );

        try
        {
            client.multiply( 10, 5 );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {   // Good
            // TODO catch FailingException instead of Exception and make Server throw the proper
            // one instead of getting a "channel closed".
        }
    }

    @Test
    public void clientCanReadChunkSizeBiggerThanItsOwn()
    {   // Given that frameLength is the same for both client and server.
        int serverChunkSize = 20000;
        int clientChunkSize = serverChunkSize / 10;
        MadeUpServer server = builder.chunkSize( serverChunkSize ).server();
        MadeUpClient client = builder.chunkSize( clientChunkSize ).client();

        addToLifeAndStart( server, client );

        // Tell server to stream data occupying roughly two chunks. The chunks
        // from server are 10 times bigger than the clients chunk size.
        client.fetchDataStream( new ToAssertionWriter(), serverChunkSize * 2 );
    }

    @Test
    public void serverCanReadChunkSizeBiggerThanItsOwn()
    {   // Given that frameLength is the same for both client and server.
        int serverChunkSize = 1000;
        int clientChunkSize = serverChunkSize * 10;
        MadeUpServer server = builder.chunkSize( serverChunkSize ).server();
        MadeUpClient client = builder.chunkSize( clientChunkSize ).client();

        addToLifeAndStart( server, client );

        // Tell server to stream data occupying roughly two chunks. The chunks
        // from server are 10 times bigger than the clients chunk size.
        client.sendDataStream( new DataProducer( clientChunkSize * 2 ) );
    }

    @Test
    public void impossibleToHaveBiggerChunkSizeThanFrameSize() throws Throwable
    {
        Builder myBuilder = builder.chunkSize( MadeUpServer.FRAME_LENGTH + 10 );
        try
        {
            MadeUpServer server = myBuilder.server();
            server.init();
            server.start();
            fail( "Shouldn't be possible" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }

        try
        {
            myBuilder.client();
            fail( "Shouldn't be possible" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }
    }

    @Test
    public void clientShouldUseHandlersToHandleComExceptions()
    {
        // Given
        final String comExceptionMessage = "The ComException";

        MadeUpCommunicationInterface communication = mock( MadeUpCommunicationInterface.class,
                (Answer<Response<?>>) ingored ->
                {
                    throw new ComException( comExceptionMessage );
                } );

        ComExceptionHandler handler = mock( ComExceptionHandler.class );

        life.add( builder.server( communication ) );
        MadeUpClient client = life.add( builder.client() );
        client.setComExceptionHandler( handler );

        life.start();

        // When
        ComException exceptionThrownOnRequest = null;
        try
        {
            client.multiply( 1, 10 );
        }
        catch ( ComException e )
        {
            exceptionThrownOnRequest = e;
        }

        // Then
        assertNotNull( exceptionThrownOnRequest );
        assertEquals( comExceptionMessage, exceptionThrownOnRequest.getMessage() );

        ArgumentCaptor<ComException> exceptionCaptor = ArgumentCaptor.forClass( ComException.class );
        verify( handler ).handle( exceptionCaptor.capture() );
        assertEquals( comExceptionMessage, exceptionCaptor.getValue().getMessage() );
        verifyNoMoreInteractions( handler );
    }

    @Test
    @SuppressWarnings( "rawtypes" )
    public void masterResponseShouldBeUnpackedIfRequestTypeRequires() throws Exception
    {
        // Given
        ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );
        MadeUpClient client = builder.clientWith( responseUnpacker );
        addToLifeAndStart( builder.server(), client );

        // When
        client.multiply( 42, 42 );

        // Then
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass( Response.class );
        verify( responseUnpacker ).unpackResponse( captor.capture(), eq( NO_OP_TX_HANDLER ) );
        assertEquals( storeIdToUse, captor.getValue().getStoreId() );
        assertEquals( 42 * 42, captor.getValue().response() );
    }

    @Test
    public void masterResponseShouldNotBeUnpackedIfRequestTypeDoesNotRequire()
    {
        // Given
        ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );
        MadeUpClient client = builder.clientWith( responseUnpacker );
        addToLifeAndStart( builder.server(), client );

        // When
        client.sendDataStream( new KnownDataByteChannel( 100 ) );

        // Then
        verifyZeroInteractions( responseUnpacker );
    }

    @Test
    public void shouldStreamBackTransactions() throws Exception
    {
        // GIVEN
        int value = 11;
        int txCount = 5;
        life.add( builder.server() );
        MadeUpClient client = life.add( builder.client() );
        life.start();
        Response<Integer> respone = client.streamBackTransactions( value, txCount );
        TransactionStreamVerifyingResponseHandler handler = new TransactionStreamVerifyingResponseHandler( txCount );

        // WHEN
        respone.accept( handler );
        int responseValue = respone.response();

        // THEN
        assertEquals( value, responseValue );
        assertEquals( txCount, handler.expectedTxId - TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldAdhereToTransactionObligations() throws Exception
    {
        // GIVEN
        int value = 15;
        long desiredObligation = 8;
        life.add( builder.server() );
        MadeUpClient client = life.add( builder.client() );
        life.start();
        Response<Integer> respone = client.informAboutTransactionObligations( value, desiredObligation );
        TransactionObligationVerifyingResponseHandler handler = new TransactionObligationVerifyingResponseHandler();

        // WHEN
        respone.accept( handler );
        int responseValue = respone.response();

        // THEN
        assertEquals( value, responseValue );
        assertEquals( desiredObligation, handler.obligationTxId );
    }

    private void addToLifeAndStart( MadeUpServer server, MadeUpClient client )
    {
        life.add( server );
        life.add( client );
        life.init();
        life.start();
    }

    class Builder
    {
        private final int port;
        private final int chunkSize;
        private final byte internalProtocolVersion;
        private final byte applicationProtocolVersion;
        private final TxChecksumVerifier verifier;
        private final StoreId storeId;

        Builder()
        {
            this( PortAuthority.allocatePort(), FRAME_LENGTH, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION,
                    ALWAYS_MATCH, storeIdToUse );
        }

        Builder( int port, int chunkSize, byte internalProtocolVersion, byte applicationProtocolVersion,
                        TxChecksumVerifier verifier, StoreId storeId )
        {
            this.port = port;
            this.chunkSize = chunkSize;
            this.internalProtocolVersion = internalProtocolVersion;
            this.applicationProtocolVersion = applicationProtocolVersion;
            this.verifier = verifier;
            this.storeId = storeId;
        }

        public Builder port( int port )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public Builder chunkSize( int chunkSize )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public Builder internalProtocolVersion( byte internalProtocolVersion )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public Builder applicationProtocolVersion( byte applicationProtocolVersion )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public Builder verifier( TxChecksumVerifier verifier )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public Builder storeId( StoreId storeId )
        {
            return new Builder(
                    port, chunkSize, internalProtocolVersion, applicationProtocolVersion, verifier, storeId );
        }

        public MadeUpServer server()
        {
            return new MadeUpServer( new MadeUpServerImplementation( storeId ), port,
                    internalProtocolVersion, applicationProtocolVersion, verifier, chunkSize );
        }

        public MadeUpServer server( MadeUpCommunicationInterface target )
        {
            return new MadeUpServer(
                    target, port, internalProtocolVersion, applicationProtocolVersion, verifier, chunkSize );
        }

        public MadeUpClient client()
        {
            return clientWith( ResponseUnpacker.NO_OP_RESPONSE_UNPACKER );
        }

        public MadeUpClient clientWith( ResponseUnpacker responseUnpacker )
        {
            return new MadeUpClient( port, storeId, chunkSize, responseUnpacker )
            {
                @Override
                public ProtocolVersion getProtocolVersion()
                {
                    return new ProtocolVersion( applicationProtocolVersion, internalProtocolVersion );
                }
            };
        }

        public ServerInterface serverInOtherJvm()
        {
            ServerInterface server = new MadeUpServerProcess().start( new StartupData(
                    storeId.getCreationTime(), storeId.getRandomId(), internalProtocolVersion,
                    applicationProtocolVersion, chunkSize ) );
            server.awaitStarted();
            return server;
        }
    }

    public class TransactionStreamVerifyingResponseHandler
            implements Response.Handler, Visitor<CommittedTransactionRepresentation,Exception>
    {
        private final long txCount;
        private long expectedTxId = 1;

        public TransactionStreamVerifyingResponseHandler( int txCount )
        {
            this.txCount = txCount;
        }

        @Override
        public void obligation( long txId )
        {
            fail( "Should not called" );
        }

        @Override
        public Visitor<CommittedTransactionRepresentation,Exception> transactions()
        {
            return this;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation element )
        {
            assertEquals( expectedTxId + TransactionIdStore.BASE_TX_ID, element.getCommitEntry().getTxId() );
            expectedTxId++;
            assertThat( element.getCommitEntry().getTxId(), lessThanOrEqualTo( txCount + TransactionIdStore.BASE_TX_ID ) );
            return false;
        }
    }

    public class TransactionObligationVerifyingResponseHandler implements Response.Handler
    {
        volatile long obligationTxId;

        @Override
        public void obligation( long txId )
        {
            this.obligationTxId = txId;
        }

        @Override
        public Visitor<CommittedTransactionRepresentation,Exception> transactions()
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }
    }
}
