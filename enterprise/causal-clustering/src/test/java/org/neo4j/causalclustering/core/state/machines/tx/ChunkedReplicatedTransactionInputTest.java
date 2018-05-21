/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
            ByteBuf chunk = chunkedReplicatedTransactionInput.readChunk( allocator );

            ReplicatedContentChunk deserializedChunk = ReplicatedContentChunk.deSerialize( chunk );

            composedDeserialized.writeBytes( deserializedChunk.content() );
            chunk.release();
        }
        byte[] array = Arrays.copyOf( composedDeserialized.array(), composedDeserialized.readableBytes() );
        assertEquals( replicatedTransaction, new ReplicatedTransaction( array ) );
    }
}
