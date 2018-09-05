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
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalLong;

import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.WritableChannel;

public class ByteArrayReplicatedTransaction implements ReplicatedTransaction
{
    private final byte[] txBytes;

    @Override
    public OptionalLong size()
    {
        return OptionalLong.of( (long) txBytes.length );
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    ByteArrayReplicatedTransaction( byte[] txBytes )
    {
        this.txBytes = txBytes;
    }

    byte[] getTxBytes()
    {
        return txBytes;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ByteArrayReplicatedTransaction that = (ByteArrayReplicatedTransaction) o;
        return Arrays.equals( txBytes, that.txBytes );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( txBytes );
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return ReplicatedTransactionSerializer.encode( this );
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        ReplicatedTransactionSerializer.marshal( channel, this );
    }

    @Override
    public TransactionRepresentation extract( TransactionRepresentationExtractor extractor )
    {
        return extractor.extract( this );
    }
}
