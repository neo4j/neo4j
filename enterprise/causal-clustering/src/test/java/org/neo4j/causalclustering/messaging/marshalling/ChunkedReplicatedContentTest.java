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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.Integer.min;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChunkedReplicatedContentTest
{
    @Test
    public void shouldProvideExpectedMetaData() throws IOException
    {
        ChunkedReplicatedContent replicatedContent = new ChunkedReplicatedContent( (byte) 1, new ThreeChunks( -1, 8 ) );

        UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

        ByteBuf byteBuf = replicatedContent.readChunk( allocator );

        // is not last
        assertFalse( byteBuf.readBoolean() );
        // first chunk unknown length
        assertEquals( 8, byteBuf.readInt() );
        // first chunk has content
        assertEquals( (byte) 1, byteBuf.readByte() );
        byteBuf.release();

        byteBuf = replicatedContent.readChunk( allocator );
        // is not last
        assertFalse( byteBuf.readBoolean() );
        // second chunk has length -1
        assertEquals( 8, byteBuf.readInt() );
        byteBuf.release();

        byteBuf = replicatedContent.readChunk( allocator );
        // is last
        assertTrue( byteBuf.readBoolean() );
        // third chunk has a length
        assertEquals( 8, byteBuf.readInt() );
        byteBuf.release();

        assertNull( replicatedContent.readChunk( allocator ) );
    }

    private class ThreeChunks implements ChunkedEncoder
    {
        private final int length;
        private int leftTowWrite;
        private final int chunkSize;
        private int count;

        ThreeChunks( int length, int chunkSize )
        {
            this.length = length;
            this.leftTowWrite = length == -1 ? Integer.MAX_VALUE : length;
            this.chunkSize = chunkSize;
        }

        @Override
        public ByteBuf encodeChunk( ByteBufAllocator allocator )
        {
            if ( count == 3 )
            {
                return null;
            }
            ByteBuf buffer = allocator.buffer( chunkSize, chunkSize );
            count++;
            int toWrite = min( leftTowWrite, buffer.writableBytes() );
            leftTowWrite -= toWrite;
            buffer.writerIndex( buffer.writerIndex() + toWrite );
            return buffer;
        }

        @Override
        public void marshal( WritableChannel channel )
        {
            throw new UnsupportedOperationException( "Should not be used" );
        }

        @Override
        public boolean isEndOfInput()
        {
            return count == 3;
        }
    }
}
