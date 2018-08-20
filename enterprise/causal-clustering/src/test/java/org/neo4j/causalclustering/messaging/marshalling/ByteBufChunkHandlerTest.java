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
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.helpers.Buffers;
import org.neo4j.causalclustering.messaging.MessageTooBigException;

public class ByteBufChunkHandlerTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test( expected = MessageTooBigException.class )
    public void shouldThrowExceptioIfToLarge() throws MessageTooBigException
    {
        ByteBufChunkHandler.MaxTotalSize maxTotalSize = new ByteBufChunkHandler.MaxTotalSize( 10 );
        ByteBuf buffer1 = buffers.buffer( 10 );
        ByteBuf buffer2 = buffers.buffer( 1 );

        buffer1.writerIndex( 10 );
        buffer2.writerIndex( 1 );
        maxTotalSize.handle( buffer1 );
        maxTotalSize.handle( buffer2 );
    }

    @Test
    public void shouldIgnoreNull() throws MessageTooBigException
    {
        ByteBufChunkHandler.MaxTotalSize maxTotalSize = new ByteBufChunkHandler.MaxTotalSize( 11 );
        ByteBuf buffer1 = buffers.buffer( 10 );
        ByteBuf buffer2 = buffers.buffer( 1 );

        buffer1.writerIndex( 10 );
        buffer2.writerIndex( 1 );
        maxTotalSize.handle( buffer1 );
        maxTotalSize.handle( null );
        maxTotalSize.handle( buffer2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfIllegalSizeValue()
    {
        new ByteBufChunkHandler.MaxTotalSize( -1 );
    }
}
