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
package org.neo4j.coreedge.raft.replication.tx;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTransactionSerializer
{
    public static void marshal( ReplicatedTransaction<CoreMember> transaction, WritableChannel channel ) throws IOException
    {
        UUID globalSessionId = transaction.globalSession().sessionId();
        channel.putLong( globalSessionId.getMostSignificantBits() );
        channel.putLong( globalSessionId.getLeastSignificantBits() );

        new CoreMember.CoreMemberMarshal().marshal( transaction.globalSession().owner(), channel );

        channel.putLong( transaction.localOperationId().localSessionId() );
        channel.putLong( transaction.localOperationId().sequenceNumber() );

        byte[] txBytes = transaction.getTxBytes();
        channel.putInt( txBytes.length );
        channel.put( txBytes, txBytes.length );
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException
    {
        long uuidMSB = channel.getLong();
        long uuidLSB = channel.getLong();
        UUID globalSessionId = new UUID( uuidMSB, uuidLSB );

        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( channel );

        long localSessionId = channel.getLong();
        long sequenceNumber = channel.getLong();

        int txBytesLength = channel.getInt();
        byte[] txBytes = new  byte[txBytesLength];
        channel.get( txBytes, txBytesLength );

        return new ReplicatedTransaction( txBytes, new GlobalSession( globalSessionId, owner ), new LocalOperationId( localSessionId, sequenceNumber ) );
    }

    public static void marshal( ReplicatedTransaction<CoreMember> transaction, ByteBuf buffer )
    {
        UUID globalSessionId = transaction.globalSession().sessionId();
        buffer.writeLong( globalSessionId.getMostSignificantBits() );
        buffer.writeLong( globalSessionId.getLeastSignificantBits() );

        new CoreMember.CoreMemberMarshal().marshal( transaction.globalSession().owner(), buffer );

        buffer.writeLong( transaction.localOperationId().localSessionId() );
        buffer.writeLong( transaction.localOperationId().sequenceNumber() );

        byte[] txBytes = transaction.getTxBytes();
        buffer.writeInt( txBytes.length );
        buffer.writeBytes( txBytes );
    }

    public static ReplicatedTransaction unmarshal( ByteBuf buffer )
    {
        long uuidMSB = buffer.readLong();
        long uuidLSB = buffer.readLong();
        UUID globalSessionId = new UUID( uuidMSB, uuidLSB );

        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( buffer );

        long localSessionId = buffer.readLong();
        long sequenceNumber = buffer.readLong();

        int txBytesLength = buffer.readInt();
        byte[] txBytes = new  byte[txBytesLength];
        buffer.readBytes( txBytes );

        return new ReplicatedTransaction( txBytes, new GlobalSession( globalSessionId, owner ), new LocalOperationId( localSessionId, sequenceNumber ) );
    }
}
