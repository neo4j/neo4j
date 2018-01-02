/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.jboss.netty.channel.Channel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.FakeClock;
import org.neo4j.logging.NullLogProvider;

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
        FakeClock clock = new FakeClock();
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
        FakeClock clock = new FakeClock();
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
        FakeClock clock = new FakeClock();
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
        FakeClock clock = new FakeClock();
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
        FakeClock clock = new FakeClock();
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
