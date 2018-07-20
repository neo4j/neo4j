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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.causalclustering.helpers.Buffers;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.ChunkedEncoder;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class ReplicatedTransactionTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test
    void shouldMarshallTransactionRepresenationToSameBytes() throws IOException
    {
        PhysicalTransactionRepresentation tx =
                new PhysicalTransactionRepresentation( Collections.singleton( new Command.NodeCommand( new NodeRecord( 1 ), new NodeRecord( 1 ) ) ) );

        ReplicatedTransaction byteArray = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( tx );
        TransactionRepresentationReplicatedTransaction representation = ReplicatedTransaction.from( tx );

        ByteBuf buffer1 = buffers.buffer();
        ByteBuf buffer2 = buffers.buffer();
        byteArray.marshal().marshal( new NetworkFlushableChannelNetty4( buffer1 ) );
        representation.marshal().marshal( new NetworkFlushableChannelNetty4( buffer2 ) );

        // inital length integer will differ
        buffer1.setInt( 0, 0 );
        buffer2.setInt( 0, 0 );

        assertArrayEquals( buffer1.array(), buffer2.array() );
        assertTrue( ByteBufUtil.equals( buffer1, buffer2 ) );
    }

    @Test
    void shouldEncodeToSameBytes() throws IOException
    {
        PhysicalTransactionRepresentation tx =
                new PhysicalTransactionRepresentation( Collections.singleton( new Command.NodeCommand( new NodeRecord( 1 ), new NodeRecord( 1 ) ) ) );

        ReplicatedTransaction byteArray = ReplicatedTransactionFactory.createImmutableReplicatedTransaction( tx );
        TransactionRepresentationReplicatedTransaction representation = ReplicatedTransaction.from( tx );

        UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buffer1 = buffers.buffer();
        ByteBuf buffer2 = buffers.buffer();

        ChunkedEncoder marshal1 = byteArray.marshal();
        encode( buffer1, marshal1 );

        ChunkedEncoder marshal2 = representation.marshal();
        encode( buffer2, marshal2 );

        // inital length integer will differ
        buffer1.setInt( 0, 0 );
        buffer2.setInt( 0, 0 );

        assertArrayEquals( buffer1.array(), buffer2.array() );

        assertTrue( ByteBufUtil.equals( buffer1, buffer2 ) );
    }

    static void encode( ByteBuf buffer, ChunkedEncoder marshal ) throws IOException
    {
        while ( !marshal.isEndOfInput() )
        {
            ByteBuf tmp = marshal.encodeChunk( UnpooledByteBufAllocator.DEFAULT );
            if ( tmp != null )
            {
                buffer.writeBytes( tmp );
                tmp.release();
            }
        }
    }
}
