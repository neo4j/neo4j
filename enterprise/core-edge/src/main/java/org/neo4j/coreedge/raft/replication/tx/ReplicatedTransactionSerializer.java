/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.UUID;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.membership.CoreMemberMarshal;
import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;

public class ReplicatedTransactionSerializer
{
    public static void serialize( ReplicatedTransaction transaction, ByteBuf buffer ) throws MarshallingException
    {
        UUID globalSessionId = transaction.globalSession().sessionId();
        buffer.writeLong( globalSessionId.getMostSignificantBits() );
        buffer.writeLong( globalSessionId.getLeastSignificantBits() );

        CoreMemberMarshal.serialize( transaction.globalSession().owner(), buffer );

        buffer.writeLong( transaction.localOperationId().localSessionId() );
        buffer.writeLong( transaction.localOperationId().sequenceNumber() );

        byte[] txBytes = transaction.getTxBytes();
        buffer.writeInt( txBytes.length );
        buffer.writeBytes( txBytes );
    }

    public static ReplicatedTransaction deserialize( ByteBuf buffer ) throws MarshallingException
    {
        long uuidMSB = buffer.readLong();
        long uuidLSB = buffer.readLong();
        UUID globalSessionId = new UUID( uuidMSB, uuidLSB );

        CoreMember owner = CoreMemberMarshal.deserialize( buffer );

        long localSessionId = buffer.readLong();
        long sequenceNumber = buffer.readLong();

        int txBytesLength = buffer.readInt();
        byte[] txBytes = new  byte[txBytesLength];
        buffer.readBytes( txBytes, 0, txBytesLength );

        return new ReplicatedTransaction( txBytes, new GlobalSession( globalSessionId, owner ), new LocalOperationId( localSessionId, sequenceNumber ) );
    }
}
