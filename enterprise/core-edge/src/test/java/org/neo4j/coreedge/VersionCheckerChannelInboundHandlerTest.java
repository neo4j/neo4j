/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.neo4j.coreedge.messaging.BaseMessage;
import org.neo4j.coreedge.messaging.Message;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertSame;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class VersionCheckerChannelInboundHandlerTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldDiscardMessageWithUnknownVersionAndLogAnError() throws Exception
    {
        // given
        Predicate<Message> versionChecker = ( m ) -> false;
        VersionCheckerChannelInboundHandler<Message> handler =
                new VersionCheckerChannelInboundHandler<Message>( versionChecker, logProvider )
                {
                    @Override
                    protected void doChannelRead0( ChannelHandlerContext ctx, Message message ) throws Exception
                    {
                        throw new UnsupportedOperationException( "nope" );
                    }
                };

        // when
        BaseMessage message = new BaseMessage( (byte) 42 );
        handler.channelRead0( null, message );

        // then
        logProvider.assertExactly( inLog( handler.getClass() )
                .error( "Unsupported version %d, unable to process message %s", message.version(), message ) );
    }

    @Test
    public void shouldHandleMessageWithCorrectVersion() throws Exception
    {
        // given
        Predicate<Message> versionChecker = ( m ) -> true;
        AtomicReference<Message> processedMessage = new AtomicReference<>();
        VersionCheckerChannelInboundHandler<Message> handler =
                new VersionCheckerChannelInboundHandler<Message>( versionChecker, logProvider )
                {
                    @Override
                    protected void doChannelRead0( ChannelHandlerContext ctx, Message message ) throws Exception
                    {
                        processedMessage.set( message );
                    }
                };

        // when
        BaseMessage message = new BaseMessage( (byte) 42 );
        handler.channelRead0( null, message );

        // then
        logProvider.assertNoLoggingOccurred();
        assertSame( message, processedMessage.get() );
    }
}
