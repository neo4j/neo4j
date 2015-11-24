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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class LockMessageMarshallTest
{
    @Test
    public void shouldSerializeAcquireExclusiveLockRequest()
    {
        ByteBuf buffer = Unpooled.buffer();
        // given
        CoreMember owner = new CoreMember( address( "localhost:1000" ), address( "localhost:2000" ) );

        // when
        LockMessage.Request original = LockMessage.Request.acquireExclusiveLock( new LockSession( owner, 0 ), ResourceTypes.NODE, 0 );
        LockMessageMarshall.serialize( buffer, original );
        LockMessage.Request lockRequest = LockMessageMarshall.deserializeRequest( buffer );

        // then
        assertEquals( original, lockRequest );
    }

    @Test
    public void shouldSerializeAcquireSharedLockRequest()
    {
        ByteBuf buffer = Unpooled.buffer();
        // given
        CoreMember owner = new CoreMember( address( "localhost:1000" ), address( "localhost:2000" ) );

        // when
        LockMessage.Request original = LockMessage.Request.acquireSharedLock( new LockSession( owner, 0 ), ResourceTypes.NODE, 0 );
        LockMessageMarshall.serialize( buffer, original );
        LockMessage.Request lockRequest = LockMessageMarshall.deserializeRequest( buffer );

        // then
        assertEquals( original, lockRequest );
    }

    @Test
    public void shouldSerializeNewLockSessionRequest()
    {
        ByteBuf buffer = Unpooled.buffer();
        // given
        CoreMember owner = new CoreMember( address( "localhost:1000" ), address( "localhost:2000" ) );

        // when
        LockMessage.Request original = LockMessage.Request.newLockSession( new LockSession( owner, 0 ) );
        LockMessageMarshall.serialize( buffer, original );
        LockMessage.Request lockRequest = LockMessageMarshall.deserializeRequest( buffer );

        // then
        assertEquals( original, lockRequest );
    }

    @Test
    public void shouldSerializeEndLockSessionRequest()
    {
        ByteBuf buffer = Unpooled.buffer();
        // given
        CoreMember owner = new CoreMember( address( "localhost:1000" ), address( "localhost:2000" ) );

        // when
        LockMessage.Request original = LockMessage.Request.endLockSession( new LockSession( owner, 0 ) );
        LockMessageMarshall.serialize( buffer, original );
        LockMessage.Request lockRequest = LockMessageMarshall.deserializeRequest( buffer );

        // then
        assertEquals( original, lockRequest );
    }

    @Test
    public void shouldSerializeAcquireExclusiveLockResponse()
    {
        ByteBuf buffer = Unpooled.buffer();
        // given
        CoreMember owner = new CoreMember( address( "localhost:1000" ), address( "localhost:2000" ) );

        // when
        LockMessage.Request request = LockMessage.Request.acquireExclusiveLock( new LockSession( owner, 0 ), ResourceTypes.NODE, 0 );
        LockMessage.Response original = new LockMessage.Response( request, new LockResult( LockStatus.OK_LOCKED ) );


        LockMessageMarshall.serialize( buffer, original );
        LockMessage.Response lockResponse = LockMessageMarshall.deserializeResponse( buffer );

        // then
        assertEquals( original, lockResponse );
    }
}
