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
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.causalclustering.messaging.marshalling.ChunkedReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentChunk;

import static org.junit.Assert.assertEquals;

public class ChunkedReplicatedTransactionInputTest
{

    @Test
    public void shouldEncodeAndDecode() throws Exception
    {
        ReplicatedTransaction replicatedTransaction = new ReplicatedTransaction( new byte[]{1, 2, 3, 4} );
        byte contentType = (byte) 1;
        ChunkedReplicatedContent chunkedReplicatedTransactionInput =
                new ChunkedReplicatedContent( contentType, ReplicatedTransactionSerializer.serializer( replicatedTransaction ), 4 );

        UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf composedDeserialized = Unpooled.buffer();
        while ( !chunkedReplicatedTransactionInput.isEndOfInput() )
        {
            ReplicatedContentChunk chunk = chunkedReplicatedTransactionInput.readChunk( allocator );

            ByteBuf buffer = Unpooled.buffer();
            chunk.encode( buffer );

            ReplicatedContentChunk deserializedChunk = ReplicatedContentChunk.deSerialize( buffer );

            composedDeserialized.writeBytes( deserializedChunk.content() );
            buffer.release();
        }
        byte[] array = Arrays.copyOf( composedDeserialized.array(), composedDeserialized.readableBytes() );
        assertEquals( replicatedTransaction, new ReplicatedTransaction( array ) );
    }
}
