/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

public class TxPullResponseDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out ) throws Exception
    {
        NetworkReadableClosableChannelNetty4 logChannel = new NetworkReadableClosableChannelNetty4( msg );
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( logChannel );
        LogEntryReader<NetworkReadableClosableChannelNetty4> reader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );
        PhysicalTransactionCursor<NetworkReadableClosableChannelNetty4> transactionCursor =
                new PhysicalTransactionCursor<>( logChannel, reader );

        transactionCursor.next();
        CommittedTransactionRepresentation tx = transactionCursor.get();

        if ( tx != null )
        {
            out.add( new TxPullResponse( storeId, tx ) );
        }
    }
}
