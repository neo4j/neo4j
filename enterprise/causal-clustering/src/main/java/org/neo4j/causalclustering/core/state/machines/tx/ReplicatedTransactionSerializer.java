/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTransactionSerializer
{
    private ReplicatedTransactionSerializer()
    {
    }

    public static void marshal( ReplicatedTransaction transaction, WritableChannel channel ) throws IOException
    {
        byte[] txBytes = transaction.getTxBytes();
        channel.putInt( txBytes.length );
        channel.put( txBytes, txBytes.length );
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException
    {
        int txBytesLength = channel.getInt();
        byte[] txBytes = new  byte[txBytesLength];
        channel.get( txBytes, txBytesLength );

        return new ReplicatedTransaction( txBytes );
    }

    public static void marshal( ReplicatedTransaction transaction, ByteBuf buffer )
    {
        byte[] txBytes = transaction.getTxBytes();
        buffer.writeInt( txBytes.length );
        buffer.writeBytes( txBytes );
    }

    public static ReplicatedTransaction unmarshal( ByteBuf buffer )
    {
        int txBytesLength = buffer.readInt();
        byte[] txBytes = new  byte[txBytesLength];
        buffer.readBytes( txBytes );

        return new ReplicatedTransaction( txBytes );
    }
}
