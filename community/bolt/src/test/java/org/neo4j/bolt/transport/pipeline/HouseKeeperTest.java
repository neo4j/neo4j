/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class HouseKeeperTest
{
    private EmbeddedChannel channel;

    @After
    public void cleanup()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldStopConnectionOnChannelInactive()
    {
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLogService.getInstance() ) );

        channel.pipeline().fireChannelInactive();

        verify( connection ).stop();
    }

    @Test
    public void shouldNotPropagateChannelInactive() throws Exception
    {
        ChannelInboundHandler next = mock( ChannelInboundHandler.class );
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLogService.getInstance() ), next );

        channel.pipeline().fireChannelInactive();

        verify( next, never() ).channelInactive( any() );
    }

    @Test
    public void shouldStopConnectionOnExceptionCaught()
    {
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLogService.getInstance() ) );

        channel.pipeline().fireExceptionCaught( new RuntimeException( "some exception" ) );

        verify( connection ).stop();
    }

    @Test
    public void shouldLogExceptionOnExceptionCaught()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, new SimpleLogService( logProvider ) ) );

        RuntimeException exception = new RuntimeException( "some exception" );
        channel.pipeline().fireExceptionCaught( exception );

        verify( connection ).stop();
        logProvider.assertExactly(
                inLog( HouseKeeper.class ).error( startsWith( "Fatal error occurred when handling a client connection" ), equalTo( exception ) ) );
    }

    @Test
    public void shouldNotPropagateExceptionCaught() throws Exception
    {
        ChannelInboundHandler next = mock( ChannelInboundHandler.class );
        BoltConnection connection = mock( BoltConnection.class );
        channel = new EmbeddedChannel( new HouseKeeper( connection, NullLogService.getInstance() ), next );

        channel.pipeline().fireExceptionCaught( new RuntimeException( "some exception" ) );

        verify( next, never() ).exceptionCaught( any(), any() );
    }
}
