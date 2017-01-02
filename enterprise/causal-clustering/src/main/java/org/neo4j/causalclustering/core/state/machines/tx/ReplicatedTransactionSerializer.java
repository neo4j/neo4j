/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTransactionSerializer
{
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
