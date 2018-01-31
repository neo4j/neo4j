/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.kernel.impl.logging.NullLogService;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBoltProtocolHandlerFactoryTest
{
    @Test
    public void shouldCreateV1Handler()
    {
        int protocolVersion = 1;
        BoltChannel boltChannel = BoltChannel.open( newChannelCtxMock(), NullBoltMessageLogger.getInstance() );
        WorkerFactory workerFactory = mock( WorkerFactory.class );

        BoltWorker worker = mock( BoltWorker.class );
        when( workerFactory.newWorker( same( boltChannel ), any() ) ).thenReturn( worker );

        BoltProtocolHandlerFactory factory = new DefaultBoltProtocolHandlerFactory( workerFactory,
                TransportThrottleGroup.NO_THROTTLE, NullLogService.getInstance() );

        BoltMessagingProtocolHandler handler = factory.create( protocolVersion, boltChannel );

        // handler is actually created
        assertNotNull( handler );
        // it uses the expected worker
        verify( workerFactory ).newWorker( same( boltChannel ), any() );

        // and halts this same worker when closed
        verify( worker, never() ).halt();
        handler.close();
        verify( worker ).halt();
    }

    @Test
    public void shouldCreateNothingForUnknownProtocolVersion()
    {
        int protocolVersion = 42;
        BoltChannel channel = mock( BoltChannel.class );
        BoltProtocolHandlerFactory factory = new DefaultBoltProtocolHandlerFactory( mock( WorkerFactory.class ),
                TransportThrottleGroup.NO_THROTTLE, NullLogService.getInstance() );

        BoltMessagingProtocolHandler handler = factory.create( protocolVersion, channel );

        // handler is not created
        assertNull( handler );
    }

    private static ChannelHandlerContext newChannelCtxMock()
    {
        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        Channel channel = mock( Channel.class, RETURNS_MOCKS );
        when( ctx.channel() ).thenReturn( channel );
        return ctx;
    }
}
