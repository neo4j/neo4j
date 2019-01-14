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
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.graphdb.TransactionTerminatedException;
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
        doThrow( new RuntimeException( "Something went horribly wrong" ) ).when( msgHandler ).onSuccess(
                any( MapValue.class ) );

        BoltConnection connection = mock( BoltConnection.class );
        MessageProcessingHandler handler =
                new MessageProcessingHandler( msgHandler, connection, mock( Log.class ) );

        // When
        handler.onFinish();

        // Then
        verify( connection ).stop();
    }

    @Test
    public void shouldLogOriginalErrorWhenOutputIsClosed() throws Exception
    {
        testLoggingOfOriginalErrorWhenOutputIsClosed( Neo4jError.from( new RuntimeException( "Non-fatal error" ) ) );
    }

    @Test
    public void shouldLogOriginalFatalErrorWhenOutputIsClosed() throws Exception
    {
        testLoggingOfOriginalErrorWhenOutputIsClosed( Neo4jError.fatalFrom( new RuntimeException( "Fatal error" ) ) );
    }

    @Test
    public void shouldLogWriteErrorAndOriginalErrorWhenUnknownFailure() throws Exception
    {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(
                Neo4jError.from( new RuntimeException( "Non-fatal error" ) ) );
    }

    @Test
    public void shouldLogWriteErrorAndOriginalFatalErrorWhenUnknownFailure() throws Exception
    {
        testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure(
                Neo4jError.fatalFrom( new RuntimeException( "Fatal error" ) ) );
    }

    @Test
    public void shouldLogShortWarningOnClientDisconnectMidwayThroughQuery() throws Exception
    {
        // Connections dying is not exceptional per-se, so we don't need to fill the log with
        // eye-catching stack traces; but it could be indicative of some issue, so log a brief
        // warning in the debug log at least.

        // Given
        PackOutputClosedException outputClosed = new PackOutputClosedException( "Output closed", "<client>" );
        Neo4jError txTerminated =
                Neo4jError.from( new TransactionTerminatedException( Status.Transaction.Terminated ) );

        // When
        AssertableLogProvider logProvider = emulateFailureWritingError( txTerminated, outputClosed );

        // Then
        logProvider.assertExactly( inLog( "Test" ).warn( equalTo(
                "Client %s disconnected while query was running. Session has been cleaned up. " +
                        "This can be caused by temporary network problems, but if you see this often, ensure your " +
                        "applications are properly waiting for operations to complete before exiting." ),
                equalTo( "<client>" ) ) );
    }

    private static void testLoggingOfOriginalErrorWhenOutputIsClosed( Neo4jError original ) throws Exception
    {
        PackOutputClosedException outputClosed = new PackOutputClosedException( "Output closed", "<client>" );
        AssertableLogProvider logProvider = emulateFailureWritingError( original, outputClosed );
        logProvider.assertExactly( inLog( "Test" ).warn( startsWith( "Unable to send error back to the client" ),
                equalTo( original.cause() ) ) );
    }

    private static void testLoggingOfWriteErrorAndOriginalErrorWhenUnknownFailure( Neo4jError original )
            throws Exception
    {
        RuntimeException outputError = new RuntimeException( "Output failed" );
        AssertableLogProvider logProvider = emulateFailureWritingError( original, outputError );
        logProvider.assertExactly( inLog( "Test" ).error( startsWith( "Unable to send error back to the client" ),
                both( equalTo( outputError ) ).and( hasSuppressed( original.cause() ) ) ) );
    }

    private static AssertableLogProvider emulateFailureWritingError( Neo4jError error, Throwable errorDuringWrite )
            throws Exception
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BoltResponseMessageHandler<IOException> responseHandler =
                newResponseHandlerMock( error.isFatal(), errorDuringWrite );

        MessageProcessingHandler handler =
                new MessageProcessingHandler( responseHandler, mock( BoltConnection.class ),
                        logProvider.getLog( "Test" ) );

        handler.markFailed( error );
        handler.onFinish();

        return logProvider;
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
