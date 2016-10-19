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
package org.neo4j.causalclustering.core.consensus.membership;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MemberIdMarshalTest
{
    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // given
        MemberId.Marshal marshal = new MemberId.Marshal();

        final MemberId member = new MemberId( UUID.randomUUID() );

        // when
        ByteBuf buffer = Unpooled.buffer( 1_000 );
        marshal.marshal( member, new NetworkFlushableChannelNetty4( buffer ) );
        final MemberId recovered = marshal.unmarshal( new NetworkReadableClosableChannelNetty4( buffer ) );

        // then
        assertEquals( member, recovered );
    }

    @Test
    public void shouldThrowExceptionForHalfWrittenInstance() throws Exception
    {
        // given
        // a CoreMember and a ByteBuffer to write it to
        MemberId.Marshal marshal = new MemberId.Marshal();
        final MemberId aRealMember = new MemberId( UUID.randomUUID() );

        ByteBuf buffer = Unpooled.buffer( 1000 );

        // and the CoreMember is serialized but for 5 bytes at the end
        marshal.marshal( aRealMember, new NetworkFlushableChannelNetty4( buffer ) );
        ByteBuf bufferWithMissingBytes = buffer.copy( 0, buffer.writerIndex() - 5 );

        // when
        try
        {
            marshal.unmarshal( new NetworkReadableClosableChannelNetty4( bufferWithMissingBytes ) );
            fail( "Should have thrown exception" );
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
