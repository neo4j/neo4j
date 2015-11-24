/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.locks;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.catchup.CatchupClientProtocol;
import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.raft.locks.LockMessage.Request;
import org.neo4j.coreedge.raft.locks.LockMessage.Response;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;
import static org.neo4j.coreedge.catchup.CatchupServerProtocol.NextMessage;

public class LockMessageEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodeNewLockSessionRequest()
    {
        CatchupServerProtocol protocol = new CatchupServerProtocol();
        protocol.expect( NextMessage.LOCK_REQUEST );

        EmbeddedChannel channel = new EmbeddedChannel( new LockRequestEncoder(),
                new LockRequestDecoder( protocol ) );

        // given
        LockSession lockSession = new LockSession(
                new CoreMember( address( "host1:1001" ), address( "host1:2001" ) ), 23 );
        Request sent = Request.newLockSession( lockSession );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( channel.readOutbound() );

        // then
        Request received = (Request) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    @Test
    public void shouldEncodeAndDecodeAcquireExclusiveLockRequest()
    {
        CatchupServerProtocol protocol = new CatchupServerProtocol();
        protocol.expect( NextMessage.LOCK_REQUEST );

        EmbeddedChannel channel = new EmbeddedChannel( new LockRequestEncoder(),
                new LockRequestDecoder( protocol ) );

        // given
        LockSession lockSession = new LockSession(
                new CoreMember( address( "host1:1001" ), address( "host1:2001" ) ), 23 );
        Request sent = Request.acquireExclusiveLock( lockSession, ResourceTypes.NODE, 2001 );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( channel.readOutbound() );

        // then
        Request received = (Request) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    @Test
    public void shouldEncodeAndDecodeAcquireExclusiveLockResponse()
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();
        protocol.expect( CatchupClientProtocol.NextMessage.LOCK_RESPONSE );

        EmbeddedChannel channel = new EmbeddedChannel( new LockResponseEncoder(),
                new LockResponseDecoder( protocol ) );

        // given
        LockSession lockSession = new LockSession(
                new CoreMember( address( "host1:1001" ), address( "host1:2001" ) ), 23 );
        Request request = Request.acquireExclusiveLock( lockSession, ResourceTypes.NODE, 2001 );
        Response sent = new Response( request, new LockResult( LockStatus.OK_LOCKED ) );

        // when
        channel.writeOutbound( sent );
        channel.writeInbound( channel.readOutbound() );

        // then
        Response received = (Response) channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

}