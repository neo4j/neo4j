/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.matchers.CommonMatchers.hasSuppressed;

public class MessageProcessingHandlerTest
{
    @Test
    public void shouldCallHaltOnUnexpectedFailures() throws Exception
    {
        // Given
        BoltResponseMessageHandler<IOException> msgHandler = newResponseHandlerMock();
        doThrow( new RuntimeException( "Something went horribly wrong" ) )
                .when( msgHandler )
                .onSuccess( any( MapValue.class ) );

        BoltWorker worker = mock( BoltWorker.class );
        MessageProcessingHandler handler =
                new MessageProcessingHandler( msgHandler, mock( Runnable.class ),
                        worker, mock( Log.class ) );

        // When
        handler.onFinish();

        // Then
        verify( worker ).halt();
    }

    @Test
    public void shouldLogOriginalErrorWhenOutputIsClosed() throws Exception
    {
        testLoggingOfOriginalErrorWhenOutputIsClosed( false );
    }

    @Test
    public void shouldLogOriginalFatalErrorWhenOutputIsClosed() throws Exception
    {
        testLoggingOfOriginalErrorWhenOutputIsClosed( true );
    }

    @Test
    public void shouldLogWriteErrorAndOriginalErrorWhenUnknownFailure() throws Exception
    {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure( false );
    }

    @Test
    public void shouldLogWriteErrorAndOriginalFatalErrorWhenUnknownFailure() throws Exception
    {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure( true );
    }

    private static void testLoggingOfOriginalErrorWhenOutputIsClosed( boolean fatalError ) throws Exception
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( "Test" );

        PackOutputClosedException outputClosed = new PackOutputClosedException( "Output closed" );
        BoltResponseMessageHandler<IOException> responseHandler = newResponseHandlerMock( fatalError, outputClosed );

        MessageProcessingHandler handler = new MessageProcessingHandler( responseHandler, mock( Runnable.class ),
                mock( BoltWorker.class ), log );

        RuntimeException originalError = new RuntimeException( "Hi, I'm the original error" );
        markFailed( handler, fatalError, originalError );

        logProvider.assertExactly( inLog( "Test" ).warn(
                startsWith( "Unable to send error back to the client" ),
                equalTo( originalError ) ) );
    }

    private static void testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure( boolean fatalError ) throws Exception
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( "Test" );

        RuntimeException outputError = new RuntimeException( "Output failed" );
        BoltResponseMessageHandler<IOException> responseHandler = newResponseHandlerMock( fatalError, outputError );

        MessageProcessingHandler handler = new MessageProcessingHandler( responseHandler, mock( Runnable.class ),
                mock( BoltWorker.class ), log );

        RuntimeException originalError = new RuntimeException( "Hi, I'm the original error" );
        markFailed( handler, fatalError, originalError );

        logProvider.assertExactly( inLog( "Test" ).error(
                startsWith( "Unable to send error back to the client" ),
                both( equalTo( outputError ) ).and( hasSuppressed( originalError ) ) ) );
    }

    private static void markFailed( MessageProcessingHandler handler, boolean fatalError, Throwable error )
    {
        Neo4jError neo4jError = fatalError ? Neo4jError.fatalFrom( error ) : Neo4jError.from( error );
        handler.markFailed( neo4jError );
        handler.onFinish();
    }

    private static BoltResponseMessageHandler<IOException> newResponseHandlerMock( boolean fatalError, Throwable error )
            throws Exception
    {
        BoltResponseMessageHandler<IOException> handler = newResponseHandlerMock();
        if ( fatalError )
        {
            doThrow( error ).when( handler ).onFatal( any( Status.class ), anyString() );
        }
        else
        {
            doThrow( error ).when( handler ).onFailure( any( Status.class ), anyString() );
        }
        return handler;
    }

    @SuppressWarnings( "unchecked" )
    private static BoltResponseMessageHandler<IOException> newResponseHandlerMock()
    {
        return mock( BoltResponseMessageHandler.class );
    }
}
