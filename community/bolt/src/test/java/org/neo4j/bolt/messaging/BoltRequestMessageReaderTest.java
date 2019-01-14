/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.messaging;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import org.neo4j.bolt.messaging.Neo4jPack.Unpacker;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BoltRequestMessageReaderTest
{
    @Test
    public void shouldPropagateFatalError() throws Exception
    {
        Unpacker unpacker = mock( Unpacker.class );
        RuntimeException error = new RuntimeException();
        when( unpacker.unpackStructHeader() ).thenThrow( error );

        BoltRequestMessageReader reader = new TestBoltRequestMessageReader( connectionMock(), responseHandlerMock(), emptyList() );

        RuntimeException e = assertThrows( RuntimeException.class, () -> reader.read( unpacker ) );
        assertEquals( error, e );
    }

    @Test
    public void shouldHandleErrorThatCausesFailureMessage() throws Exception
    {
        Unpacker unpacker = mock( Unpacker.class );
        BoltIOException error = new BoltIOException( Status.General.UnknownError, "Hello" );
        when( unpacker.unpackStructHeader() ).thenThrow( error );

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltConnection connection = new SynchronousBoltConnection( stateMachine );

        BoltResponseHandler externalErrorResponseHandler = responseHandlerMock();
        BoltRequestMessageReader reader = new TestBoltRequestMessageReader( connection, externalErrorResponseHandler, emptyList() );

        reader.read( unpacker );

        verify( stateMachine ).handleExternalFailure( Neo4jError.from( error ), externalErrorResponseHandler );
    }

    @Test
    public void shouldThrowForUnknownMessage() throws Exception
    {
        Unpacker unpacker = mock( Unpacker.class );
        when( unpacker.unpackStructSignature() ).thenReturn( 'a' );

        RequestMessageDecoder decoder = new TestRequestMessageDecoder( 'b', responseHandlerMock(), mock( RequestMessage.class ) );
        BoltRequestMessageReader reader = new TestBoltRequestMessageReader( connectionMock(), responseHandlerMock(), singletonList( decoder ) );

        BoltIOException e = assertThrows( BoltIOException.class, () -> reader.read( unpacker ) );
        assertEquals( Status.Request.InvalidFormat, e.status() );
        assertFalse( e.causesFailureMessage() );
        assertEquals( "Message 0x61 is not a valid message signature.", e.getMessage() );
    }

    @Test
    public void shouldDecodeKnownMessage() throws Exception
    {
        Unpacker unpacker = mock( Unpacker.class );
        when( unpacker.unpackStructSignature() ).thenReturn( 'a' );

        RequestMessage message = mock( RequestMessage.class );
        BoltResponseHandler responseHandler = responseHandlerMock();
        RequestMessageDecoder decoder = new TestRequestMessageDecoder( 'a', responseHandler, message );

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltConnection connection = new SynchronousBoltConnection( stateMachine );

        BoltRequestMessageReader reader = new TestBoltRequestMessageReader( connection, responseHandlerMock(), singletonList( decoder ) );

        reader.read( unpacker );

        verify( stateMachine ).process( message, responseHandler );
    }

    private static BoltConnection connectionMock()
    {
        return mock( BoltConnection.class );
    }

    private static BoltResponseHandler responseHandlerMock()
    {
        return mock( BoltResponseHandler.class );
    }

    private static class TestBoltRequestMessageReader extends BoltRequestMessageReader
    {
        TestBoltRequestMessageReader( BoltConnection connection, BoltResponseHandler externalErrorResponseHandler, List<RequestMessageDecoder> decoders )
        {
            super( connection, externalErrorResponseHandler, decoders );
        }
    }

    private static class TestRequestMessageDecoder implements RequestMessageDecoder
    {
        final int signature;
        final BoltResponseHandler responseHandler;
        final RequestMessage message;

        TestRequestMessageDecoder( int signature, BoltResponseHandler responseHandler, RequestMessage message )
        {
            this.signature = signature;
            this.responseHandler = responseHandler;
            this.message = message;
        }

        @Override
        public int signature()
        {
            return signature;
        }

        @Override
        public BoltResponseHandler responseHandler()
        {
            return responseHandler;
        }

        @Override
        public RequestMessage decode( Unpacker unpacker ) throws IOException
        {
            return message;
        }
    }
}
