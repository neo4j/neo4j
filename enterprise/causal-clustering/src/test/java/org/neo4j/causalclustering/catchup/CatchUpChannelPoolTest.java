/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;


import org.junit.Test;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CatchUpChannelPoolTest
{
    @Test
    public void shouldReUseAChannelThatWasReleased() throws Exception
    {
        // given
        CatchUpChannelPool<TestChannel> pool = new CatchUpChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        pool.release( channelA );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertSame( channelA, channelB );
    }

    @Test
    public void shouldCreateANewChannelIfFirstChannelIsDisposed() throws Exception
    {
        // given
        CatchUpChannelPool<TestChannel> pool = new CatchUpChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        pool.dispose( channelA );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertNotSame( channelA, channelB );
    }

    @Test
    public void shouldCreateANewChannelIfFirstChannelIsStillActive() throws Exception
    {
        // given
        CatchUpChannelPool<TestChannel> pool = new CatchUpChannelPool<>( TestChannel::new );

        // when
        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );

        // then
        assertNotSame( channelA, channelB );
    }

    @Test
    public void shouldCleanUpOnClose() throws Exception
    {
        // given
        CatchUpChannelPool<TestChannel> pool = new CatchUpChannelPool<>( TestChannel::new );

        TestChannel channelA = pool.acquire( localAddress( 1 ) );
        TestChannel channelB = pool.acquire( localAddress( 1 ) );
        TestChannel channelC = pool.acquire( localAddress( 1 ) );

        pool.release( channelA );
        pool.release( channelC );

        TestChannel channelD = pool.acquire( localAddress( 2 ) );
        TestChannel channelE = pool.acquire( localAddress( 2 ) );
        TestChannel channelF = pool.acquire( localAddress( 2 ) );

        // when
        pool.close();

        // then
        assertTrue( channelA.closed );
        assertTrue( channelB.closed );
        assertTrue( channelC.closed );
        assertTrue( channelD.closed );
        assertTrue( channelE.closed );
        assertTrue( channelF.closed );
    }

    private static class TestChannel implements CatchUpChannelPool.Channel
    {
        private final AdvertisedSocketAddress address;
        private boolean closed;

        TestChannel( AdvertisedSocketAddress address )
        {
            this.address = address;
        }

        @Override
        public AdvertisedSocketAddress destination()
        {
            return address;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static AdvertisedSocketAddress localAddress( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }
}
