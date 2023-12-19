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

import org.jboss.netty.channel.Channel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IdleChannelReaperTest
{
    private static final int THRESHOLD = 100;
    private static final NullLogProvider NO_LOGGING = NullLogProvider.getInstance();

    @Test
    public void shouldNotCloseAnyRecentlyActiveChannels()
    {
        // given
        FakeClock clock = Clocks.fakeClock();
        ChannelCloser channelCloser = mock( ChannelCloser.class );
        IdleChannelReaper idleChannelReaper = new IdleChannelReaper( channelCloser, NO_LOGGING, clock, THRESHOLD );

        Channel channel = mock( Channel.class );
        idleChannelReaper.add( channel, dummyRequestContext() );

        // when
        idleChannelReaper.run();

        // then
        verifyNoMoreInteractions( channelCloser );
    }

    @Test
    public void shouldCloseAnyChannelsThatHaveBeenIdleForLongerThanThreshold()
    {
        // given
        FakeClock clock = Clocks.fakeClock();
        ChannelCloser channelCloser = mock( ChannelCloser.class );
        IdleChannelReaper idleChannelReaper = new IdleChannelReaper( channelCloser, NO_LOGGING, clock, THRESHOLD );

        Channel channel = mock( Channel.class );
        idleChannelReaper.add( channel, dummyRequestContext() );

        // when
        clock.forward( THRESHOLD + 1, TimeUnit.MILLISECONDS );
        idleChannelReaper.run();

        // then
        verify( channelCloser ).tryToCloseChannel( channel );
    }

    @Test
    public void shouldNotCloseAChannelThatHasBeenIdleForMoreThanHalfThresholdButIsStillOpenConnectedAndBound()
    {
        // given
        FakeClock clock = Clocks.fakeClock();
        ChannelCloser channelCloser = mock( ChannelCloser.class );
        IdleChannelReaper idleChannelReaper = new IdleChannelReaper( channelCloser, NO_LOGGING, clock, THRESHOLD );

        Channel channel = mock( Channel.class );
        idleChannelReaper.add( channel, dummyRequestContext() );
        when( channel.isOpen() ).thenReturn( true );
        when( channel.isConnected() ).thenReturn( true );
        when( channel.isBound() ).thenReturn( true );

        // when
        clock.forward( THRESHOLD / 2 + 10, TimeUnit.MILLISECONDS );
        idleChannelReaper.run();

        // then
        verifyNoMoreInteractions( channelCloser );
    }

    @Test
    public void shouldNotTryToCloseAChannelThatHasBeenRemoved()
    {
        // given
        FakeClock clock = Clocks.fakeClock();
        ChannelCloser channelCloser = mock( ChannelCloser.class );
        IdleChannelReaper idleChannelReaper = new IdleChannelReaper( channelCloser, NO_LOGGING, clock, THRESHOLD );

        Channel channel = mock( Channel.class );
        RequestContext request = dummyRequestContext();

        idleChannelReaper.add( channel, request );

        // when
        idleChannelReaper.remove( channel );
        clock.forward( THRESHOLD + 1, TimeUnit.MILLISECONDS );
        idleChannelReaper.run();

        // then
        verifyNoMoreInteractions( channelCloser );
    }

    @Test
    public void shouldNotTryToCloseAChannelThatWasRecentlyActive()
    {
        // given
        FakeClock clock = Clocks.fakeClock();
        ChannelCloser channelCloser = mock( ChannelCloser.class );
        IdleChannelReaper idleChannelReaper = new IdleChannelReaper( channelCloser, NO_LOGGING, clock, THRESHOLD );

        Channel channel = mock( Channel.class );
        RequestContext request = dummyRequestContext();

        idleChannelReaper.add( channel, request );

        // when
        clock.forward( THRESHOLD + 100, TimeUnit.MILLISECONDS );
        idleChannelReaper.update( channel );
        idleChannelReaper.run();

        // then
        verifyNoMoreInteractions( channelCloser );
    }

    private RequestContext dummyRequestContext()
    {
        return new RequestContext( 1, 1, 1, 1, 1 );
    }
}
