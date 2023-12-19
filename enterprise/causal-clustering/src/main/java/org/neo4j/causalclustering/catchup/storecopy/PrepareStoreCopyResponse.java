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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

public class PrepareStoreCopyResponse
{
    private final File[] files;
    private final PrimitiveLongSet indexIds;
    private final Long transactionId;
    private final Status status;

    public static PrepareStoreCopyResponse error( Status errorStatus )
    {
        if ( errorStatus == Status.SUCCESS )
        {
            throw new IllegalStateException( "Cannot create error result from state: " + errorStatus );
        }
        return new PrepareStoreCopyResponse( new File[0], Primitive.longSet(), 0L, errorStatus );
    }

    public static PrepareStoreCopyResponse success( File[] storeFiles, PrimitiveLongSet indexIds, long lastTransactionId )
    {
        return new PrepareStoreCopyResponse( storeFiles, indexIds, lastTransactionId, Status.SUCCESS );
    }

    PrimitiveLongSet getIndexIds()
    {
        return indexIds;
    }

    enum Status
    {
        SUCCESS,
        E_STORE_ID_MISMATCH,
        E_LISTING_STORE
    }

    private PrepareStoreCopyResponse( File[] files, PrimitiveLongSet indexIds, Long transactionId, Status status )
    {
        this.files = files;
        this.indexIds = indexIds;
        this.transactionId = transactionId;
        this.status = status;
    }

    public File[] getFiles()
    {
        return files;
    }

    long lastTransactionId()
    {
        return transactionId;
    }

    public Status status()
    {
        return status;
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
        PrepareStoreCopyResponse that = (PrepareStoreCopyResponse) o;
        return Arrays.equals( files, that.files ) && indexIds.equals( that.indexIds ) &&
               Objects.equals( transactionId, that.transactionId ) && status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( files, indexIds, transactionId, status );
    }

    public static class StoreListingMarshal extends SafeChannelMarshal<PrepareStoreCopyResponse>
    {
        @Override
        public void marshal( PrepareStoreCopyResponse prepareStoreCopyResponse, WritableChannel buffer ) throws IOException
        {
            buffer.putInt( prepareStoreCopyResponse.status.ordinal() );
            buffer.putLong( prepareStoreCopyResponse.transactionId );
            marshalFiles( buffer, prepareStoreCopyResponse.files );
            marshalIndexIds( buffer, prepareStoreCopyResponse.indexIds );
        }

        @Override
        protected PrepareStoreCopyResponse unmarshal0( ReadableChannel channel ) throws IOException
        {
            int ordinal = channel.getInt();
            Status status = Status.values()[ordinal];
            Long transactionId = channel.getLong();
            File[] files = unmarshalFiles( channel );
            PrimitiveLongSet indexIds = unmarshalIndexIds( channel );
            return new PrepareStoreCopyResponse( files, indexIds, transactionId, status );
        }

        private static void marshalFiles( WritableChannel buffer, File[] files ) throws IOException
        {
            buffer.putInt( files.length );
            for ( File file : files )
            {
                putBytes( buffer, file.getName() );
            }
        }

        private void marshalIndexIds( WritableChannel buffer, PrimitiveLongSet indexIds ) throws IOException
        {
            buffer.putInt( indexIds.size() );
            PrimitiveLongIterator itr = indexIds.iterator();
            while ( itr.hasNext() )
            {
                long indexId = itr.next();
                buffer.putLong( indexId );
            }
        }

        private static File[] unmarshalFiles( ReadableChannel channel ) throws IOException
        {
            int numberOfFiles = channel.getInt();
            File[] files = new File[numberOfFiles];
            for ( int i = 0; i < numberOfFiles; i++ )
            {
                files[i] = unmarshalFile( channel );
            }
            return files;
        }

        private static File unmarshalFile( ReadableChannel channel ) throws IOException
        {
            byte[] name = readBytes( channel );
            return new File( UTF8.decode( name ) );
        }

        private PrimitiveLongSet unmarshalIndexIds( ReadableChannel channel ) throws IOException
        {
            int numberOfIndexIds = channel.getInt();
            PrimitiveLongSet indexIds = Primitive.longSet( numberOfIndexIds );
            for ( int i = 0; i < numberOfIndexIds; i++ )
            {
                indexIds.add( channel.getLong() );
            }
            return indexIds;
        }

        private static void putBytes( WritableChannel buffer, String value ) throws IOException
        {
            byte[] bytes = UTF8.encode( value );
            buffer.putInt( bytes.length );
            buffer.put( bytes, bytes.length );
        }

        private static byte[] readBytes( ReadableChannel channel ) throws IOException
        {
            int bytesLength = channel.getInt();
            byte[] bytes = new byte[bytesLength];
            channel.get( bytes, bytesLength );
            return bytes;
        }
    }

    public static class Encoder extends MessageToByteEncoder<PrepareStoreCopyResponse>
    {

        @Override
        protected void encode( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyResponse prepareStoreCopyResponse, ByteBuf byteBuf )
                throws Exception
        {
            new PrepareStoreCopyResponse.StoreListingMarshal().marshal( prepareStoreCopyResponse, new NetworkFlushableChannelNetty4( byteBuf ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {

        @Override
        protected void decode( ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list ) throws Exception
        {
            list.add( new PrepareStoreCopyResponse.StoreListingMarshal().unmarshal( new NetworkReadableClosableChannelNetty4( byteBuf ) ) );
        }
    }
}
