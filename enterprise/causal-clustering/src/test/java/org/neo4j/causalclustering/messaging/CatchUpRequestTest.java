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
package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.RequestMessageType;

import static org.junit.Assert.assertEquals;

public class CatchUpRequestTest
{
    @Test
    public void shouldDeserializeAndSerialize() throws IOException
    {
        // given
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.heapBuffer();

        // when
        String id = "this id #";
        new SimpleCatchupRequest( id ).encodeMessage( new NetworkFlushableChannelNetty4( byteBuf ) );
        ByteBuf copy = byteBuf.copy();
        String decodeMessage1 = SimpleCatchupRequest.decodeMessage( copy );
        String decodeMessage2 = SimpleCatchupRequest.decodeMessage( new NetworkReadableClosableChannelNetty4( byteBuf ) );

        // then
        assertEquals( id, decodeMessage1 );
        assertEquals( id, decodeMessage2 );
    }

    private class SimpleCatchupRequest extends CatchUpRequest
    {
        private SimpleCatchupRequest( String id )
        {
            super( id );
        }

        @Override
        public RequestMessageType messageType()
        {
            return RequestMessageType.CORE_SNAPSHOT;
        }
    }
}
