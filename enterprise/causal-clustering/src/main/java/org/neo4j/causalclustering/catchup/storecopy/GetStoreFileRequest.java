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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkFlushableByteBuf;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.StoreCopyRequest;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

public class GetStoreFileRequest implements StoreCopyRequest
{
    private final StoreId expectedStoreId;
    private final File file;
    private final long requiredTransactionId;

    public GetStoreFileRequest( StoreId expectedStoreId, File file, long requiredTransactionId )
    {
        this.expectedStoreId = expectedStoreId;
        this.file = file;
        this.requiredTransactionId = requiredTransactionId;
    }

    @Override
    public long requiredTransactionId()
    {
        return requiredTransactionId;
    }

    @Override
    public StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    File file()
    {
        return file;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.STORE_FILE;
    }

    static class StoreFileRequestMarshall extends SafeChannelMarshal<GetStoreFileRequest>
    {
        @Override
        protected GetStoreFileRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
            long requiredTransactionId = channel.getLong();
            int fileNameLength = channel.getInt();
            byte[] fileNameBytes = new byte[fileNameLength];
            channel.get( fileNameBytes, fileNameLength );
            return new GetStoreFileRequest( storeId, new File( UTF8.decode( fileNameBytes ) ), requiredTransactionId );
        }

        @Override
        public void marshal( GetStoreFileRequest getStoreFileRequest, WritableChannel channel ) throws IOException
        {
            StoreIdMarshal.INSTANCE.marshal( getStoreFileRequest.expectedStoreId(), channel );
            channel.putLong( getStoreFileRequest.requiredTransactionId() );
            String name = getStoreFileRequest.file().getName();
            channel.putInt( name.length() );
            channel.put( UTF8.encode( name ), name.length() );
        }
    }

    public static class Encoder extends MessageToByteEncoder<GetStoreFileRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetStoreFileRequest msg, ByteBuf out ) throws Exception
        {
            new GetStoreFileRequest.StoreFileRequestMarshall().marshal( msg, new NetworkFlushableByteBuf( out ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GetStoreFileRequest getStoreFileRequest =
                    new GetStoreFileRequest.StoreFileRequestMarshall().unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getStoreFileRequest );
        }
    }

    @Override
    public String toString()
    {
        return "GetStoreFileRequest{" + "expectedStoreId=" + expectedStoreId + ", file=" + file.getName() + ", requiredTransactionId=" + requiredTransactionId +
                '}';
    }
}
