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

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.MessageTooBigException;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.gibiBytes;

/**
 * Handles chunks for {@link ReplicatedContent} being serialized through {@link ChunkedReplicatedContent}.
 */
public interface ByteBufChunkHandler
{
    static MaxTotalSize maxSizeHandler()
    {
        return new MaxTotalSize();
    }

    /**
     * This method is called just before the chunk is sent to the netty pipeline.
     *
     * @param byteBuf chunk of the {@link ReplicatedContent}
     * @throws IOException if the encoding should be interrupted. Use carefully and consider possible memory leak!
     */
    void handle( ByteBuf byteBuf ) throws IOException;

    class NoOp implements ByteBufChunkHandler
    {
        @Override
        public void handle( ByteBuf byteBuf )
        {
            // do nothing
        }
    }

    class MaxTotalSize implements ByteBufChunkHandler
    {
        private static final long DEFAULT_MAX_SERIALIZED_SIZE = gibiBytes( 1 );
        private final long maxSize;
        long totalSize;

        MaxTotalSize( long maxSize )
        {
            Preconditions.requirePositive( maxSize );
            this.maxSize = maxSize;
        }

        MaxTotalSize()
        {
            this.maxSize = DEFAULT_MAX_SERIALIZED_SIZE;
        }

        @Override
        public void handle( ByteBuf byteBuf ) throws MessageTooBigException
        {
            if ( byteBuf == null )
            {
                return;
            }
            int additionalBytes = byteBuf.readableBytes();
            this.totalSize += additionalBytes;
            if ( this.totalSize > maxSize )
            {
                throw new MessageTooBigException( format( "Size limit exceeded. Limit is %d, wanted to write %d, written so far %d", maxSize, additionalBytes,
                        totalSize - additionalBytes ) );
            }
        }
    }
}
