/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.com;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionStringToLong;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class TestCommunication
{
    private static final byte INTERNAL_PROTOCOL_VERSION = 0;
    private static final byte APPLICATION_PROTOCOL_VERSION = 0;

    private static final int PORT = 1234;
    private static final String PATH = "target/tmp";
    private StoreId storeIdToUse;

    @Before
    public void doBefore()
    {
        storeIdToUse = new StoreId();
        new File( PATH ).mkdirs();
    }

    @Test
    public void clientGetResponseFromServerViaComLayer() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        int value1 = 10;
        int value2 = 5;
        Response<Integer> response = client.multiply( 10, 5 );
        waitUntilResponseHasBeenWritten( server, 1000 );
        assertEquals( (Integer) (value1*value2), response.response() );
        assertTrue( serverImplementation.gotCalled() );
        assertTrue( server.responseHasBeenWritten() );
        client.shutdown();
        server.shutdown();
    }

    private MadeUpServer madeUpServer( MadeUpImplementation serverImplementation )
    {
        return madeUpServer( serverImplementation, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );
    }

    private MadeUpServer madeUpServer( MadeUpImplementation serverImplementation, byte internalVersion, byte applicationVersion )
    {
        return new MadeUpServer( serverImplementation, PORT, internalVersion, applicationVersion,
                TxChecksumVerifier.ALWAYS_MATCH );
    }

    private void waitUntilResponseHasBeenWritten( MadeUpServer server, int maxTime ) throws Exception
    {
        long time = currentTimeMillis();
        while ( !server.responseHasBeenWritten() && currentTimeMillis()-time < maxTime )
        {
            Thread.sleep( 50 );
        }
    }

    @Test
    public void makeSureClientStoreIdsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT,
                new StoreId( 10, 10, NeoStore.versionStringToLong( CommonAbstractStore.ALL_STORES_VERSION ) ),
                INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 1, 2 );
            fail();
        }
        catch ( ComException e )
        {
            // Good
        }
        finally
        {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    public void makeSureServerStoreIdsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation(
                new StoreId( 10, 10, versionStringToLong( ALL_STORES_VERSION ) ) );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 1, 2 );
            fail();
        }
        catch ( ComException e )
        {
            // Good
        }
        finally
        {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    public void makeSureClientCanStreamBigData() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        client.streamSomeData( new ToAssertionWriter(), Protocol.DEFAULT_FRAME_LENGTH*3 );

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void clientThrowsServerSideErrorMidwayThroughStreaming() throws Exception
    {
        final String failureMessage = "Just failing";
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse )
        {
            @Override
            public Response<Void> streamSomeData( MadeUpWriter writer, int dataSize )
            {
                writer.write( new FailingByteChannel( dataSize, failureMessage ) );
                return new Response<Void>( null, storeIdToUse,
                        TransactionStream.EMPTY, ResourceReleaser.NO_OP );
            }
        };
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.streamSomeData( new ToAssertionWriter(), Protocol.DEFAULT_FRAME_LENGTH*2 );
            fail( "Should have thrown " + MadeUpException.class.getSimpleName() );
        }
        catch ( MadeUpException e )
        {
            assertEquals( failureMessage, e.getMessage() );
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void communicateBetweenJvms() throws Exception
    {
        ServerInterface server = new MadeUpServerProcess().start( new StartupData(
                storeIdToUse.getCreationTime(), storeIdToUse.getRandomId(),
                storeIdToUse.getStoreVersion(), INTERNAL_PROTOCOL_VERSION,
                APPLICATION_PROTOCOL_VERSION ) );
        server.awaitStarted();
        MadeUpClient client = new MadeUpClient( MadeUpServerProcess.PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        assertEquals( (Integer)(9*5), client.multiply( 9, 5 ).response() );
        client.streamSomeData( new ToAssertionWriter(), 1024*1024*3 );

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void throwingServerSideExceptionBackToClient() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

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

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void applicationProtocolVersionsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation, INTERNAL_PROTOCOL_VERSION, (byte) (APPLICATION_PROTOCOL_VERSION+1) );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e ) { /* Good */ }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void applicationProtocolVersionsMustMatchMultiJvm() throws Exception
    {
        ServerInterface server = new MadeUpServerProcess().start( new StartupData(
                storeIdToUse.getCreationTime(), storeIdToUse.getRandomId(),
                storeIdToUse.getStoreVersion(), INTERNAL_PROTOCOL_VERSION,
                (byte) ( APPLICATION_PROTOCOL_VERSION + 1 ) ) );
        server.awaitStarted();
        MadeUpClient client = new MadeUpClient( MadeUpServerProcess.PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION,
                APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e ) { /* Good */ }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void internalProtocolVersionsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation, (byte)1, APPLICATION_PROTOCOL_VERSION );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, (byte)2, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e ) { /* Good */ }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void internalProtocolVersionsMustMatchMultiJvm() throws Exception
    {
        ServerInterface server = new MadeUpServerProcess().start( new StartupData(
                storeIdToUse.getCreationTime(), storeIdToUse.getRandomId(),
                storeIdToUse.getStoreVersion(), (byte) 1,
                APPLICATION_PROTOCOL_VERSION ) );
        server.awaitStarted();
        MadeUpClient client = new MadeUpClient( MadeUpServerProcess.PORT, storeIdToUse, (byte)2, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 10, 20 );
            fail( "Shouldn't be able to communicate with different application protocol versions" );
        }
        catch ( IllegalProtocolVersionException e ) { /* Good */ }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void serverStopsStreamingToDeadClient() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = madeUpServer( serverImplementation );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        int failAtSize = Protocol.DEFAULT_FRAME_LENGTH*2;
        ClientCrashingWriter writer = new ClientCrashingWriter( client, failAtSize );
        try
        {
            client.streamSomeData( writer, Protocol.DEFAULT_FRAME_LENGTH*10 );
            fail( "Should fail in the middle" );
        }
        catch ( ComException e )
        {   // Expected
        }
        assertTrue( writer.getSizeRead() >= failAtSize );

        long maxWaitUntil = System.currentTimeMillis()+2*1000;
        while ( !server.responseFailureEncountered() && System.currentTimeMillis() < maxWaitUntil ) Thread.currentThread().yield();
        assertTrue( "Failure writing the response should have been encountered", server.responseFailureEncountered() );
        assertFalse( "Response shouldn't have been successful", server.responseHasBeenWritten() );

        server.shutdown();
    }

    @Test
    public void serverContextVerificationCanThrowException() throws Exception
    {
        final String failureMessage = "I'm failing";
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        TxChecksumVerifier failingVerifier = new TxChecksumVerifier()
        {
            @Override
            public void assertMatch( long txId, int masterId, long checksum )
            {
                throw new FailingException( failureMessage );
            }
        };
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION, failingVerifier );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse, INTERNAL_PROTOCOL_VERSION, APPLICATION_PROTOCOL_VERSION );

        try
        {
            client.multiply( 10, 5 );
        }
        catch ( FailingException e )
        {   // Good
        }

        server.shutdown();
    }

    private <E extends Exception> void assertCause( ComException comException,
            Class<E> expectedCause, String expectedCauseMessagee )
    {
        Throwable cause = comException.getCause();
        assertTrue( expectedCause.isInstance( cause ) );
        assertEquals( expectedCauseMessagee, cause.getMessage() );
    }
}
