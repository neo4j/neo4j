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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TransportSelectionHandlerTest
{
    @Test
    public void shouldLogOnUnexpectedExceptionsAndClosesContext() throws Throwable
    {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        TransportSelectionHandler handler = new TransportSelectionHandler( null, null, false, false, logging, null, null );

        // When
        Throwable cause = new Throwable( "Oh no!" );
        handler.exceptionCaught( context, cause );

        // Then
        verify( context ).close();
        logging.assertExactly( inLog( TransportSelectionHandler.class )
                .error( equalTo( "Fatal error occurred when initialising pipeline: " + context.channel() ), sameInstance( cause ) ) );
    }

    @Test
    public void shouldLogConnectionResetErrorsAtWarningLevelAndClosesContext() throws Exception
    {
        // Given
        ChannelHandlerContext context = channelHandlerContextMock();
        AssertableLogProvider logging = new AssertableLogProvider();
        TransportSelectionHandler handler = new TransportSelectionHandler( null, null, false, false, logging, null, null );

        IOException connResetError = new IOException( "Connection reset by peer" );

        // When
        handler.exceptionCaught( context, connResetError );

        // Then
        verify( context ).close();
        logging.assertExactly( inLog( TransportSelectionHandler.class )
                .warn( "Fatal error occurred when initialising pipeline, " +
                        "remote peer unexpectedly closed connection: %s", context.channel() ) );
    }

    private static ChannelHandlerContext channelHandlerContextMock()
    {
        Channel channel = mock( Channel.class );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.channel() ).thenReturn( channel );
        return context;
    }
}
