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
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.queue.BlockingReadHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * Contains the logic for serializing requests and deserializing responses. Still missing the inverse, serializing
 * responses and deserializing requests, which is hard-coded in the server class. That should be moved over
 * eventually.
 */
public abstract class Protocol
{
    public static final int MEGA = 1024 * 1024;
    public static final int DEFAULT_FRAME_LENGTH = 16 * MEGA;
    public static final ObjectSerializer<Integer> INTEGER_SERIALIZER = ( responseObject, result ) -> result.writeInt( responseObject );
    public static final ObjectSerializer<Long> LONG_SERIALIZER =
            ( responseObject, result ) -> result.writeLong( responseObject );
    public static final ObjectSerializer<Void> VOID_SERIALIZER = ( responseObject, result ) -> {};
    public static final Deserializer<Integer> INTEGER_DESERIALIZER = ( buffer, temporaryBuffer ) -> buffer.readInt();
    public static final Deserializer<Void> VOID_DESERIALIZER = ( buffer, temporaryBuffer ) -> null;
    public static final Serializer EMPTY_SERIALIZER = buffer -> {};

    public static class TransactionRepresentationDeserializer implements Deserializer<TransactionRepresentation>
    {
        private final LogEntryReader<ReadableClosablePositionAwareChannel> reader;

        public TransactionRepresentationDeserializer( LogEntryReader<ReadableClosablePositionAwareChannel> reader )
        {
            this.reader = reader;
        }

        @Override
        public TransactionRepresentation read( ChannelBuffer buffer, ByteBuffer temporaryBuffer )
                throws IOException
        {
            NetworkReadableClosableChannel channel = new NetworkReadableClosableChannel( buffer );

            int authorId = channel.getInt();
            int masterId = channel.getInt();
            long latestCommittedTxWhenStarted = channel.getLong();
            long timeStarted = channel.getLong();
            long timeCommitted = channel.getLong();

            int headerLength = channel.getInt();
            byte[] header = new byte[headerLength];
            channel.get( header, headerLength );

            LogEntryCommand entryRead;
            List<StorageCommand> commands = new LinkedList<>();
            while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
            {
                commands.add( entryRead.getCommand() );
            }

            PhysicalTransactionRepresentation toReturn = new PhysicalTransactionRepresentation( commands );
            toReturn.setHeader( header, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                    timeCommitted, -1 );
            return toReturn;
        }
    }
    private final int chunkSize;

    /* ========================
       Static utility functions
       ======================== */
    private final byte applicationProtocolVersion;
    private final byte internalProtocolVersion;

    public Protocol( int chunkSize, byte applicationProtocolVersion, byte internalProtocolVersion )
    {
        this.chunkSize = chunkSize;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.internalProtocolVersion = internalProtocolVersion;
    }

    public static void addLengthFieldPipes( ChannelPipeline pipeline, int frameLength )
    {
        pipeline.addLast( "frameDecoder",
                new LengthFieldBasedFrameDecoder( frameLength + 4, 0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
    }

    public static void writeString( ChannelBuffer buffer, String name )
    {
        char[] chars = name.toCharArray();
        buffer.writeInt( chars.length );
        writeChars( buffer, chars );
    }

    public static void writeChars( ChannelBuffer buffer, char[] chars )
    {
        // TODO optimize?
        for ( char ch : chars )
        {
            buffer.writeChar( ch );
        }
    }

    public static String readString( ChannelBuffer buffer )
    {
        return readString( buffer, buffer.readInt() );
    }

    public static boolean readBoolean( ChannelBuffer buffer )
    {
        byte value = buffer.readByte();
        switch ( value )
        {
        case 0:
            return false;
        case 1:
            return true;
        default:
            throw new ComException( "Invalid boolean value " + value );
        }
    }

    public static String readString( ChannelBuffer buffer, int length )
    {
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = buffer.readChar();
        }
        return new String( chars );
    }

    public static void assertChunkSizeIsWithinFrameSize( int chunkSize, int frameLength )
    {
        if ( chunkSize > frameLength )
        {
            throw new IllegalArgumentException( "Chunk size " + chunkSize +
                                                " needs to be equal or less than frame length " + frameLength );
        }
    }

    public void serializeRequest( Channel channel, ChannelBuffer buffer, RequestType type, RequestContext ctx,
                                  Serializer payload ) throws IOException
    {
        buffer.clear();
        ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( buffer,
                channel, chunkSize, internalProtocolVersion, applicationProtocolVersion );
        chunkingBuffer.writeByte( type.id() );
        writeContext( ctx, chunkingBuffer );
        payload.write( chunkingBuffer );
        chunkingBuffer.done();
    }

    public <PAYLOAD> Response<PAYLOAD> deserializeResponse( BlockingReadHandler<ChannelBuffer> reader,
            ByteBuffer input, long timeout,
            Deserializer<PAYLOAD> payloadDeserializer,
            ResourceReleaser channelReleaser,
            final LogEntryReader<ReadableClosablePositionAwareChannel> entryReader ) throws IOException
    {
        final DechunkingChannelBuffer dechunkingBuffer = new DechunkingChannelBuffer( reader, timeout,
                internalProtocolVersion, applicationProtocolVersion );

        PAYLOAD response = payloadDeserializer.read( dechunkingBuffer, input );
        StoreId storeId = readStoreId( dechunkingBuffer, input );

        // Response type is what previously was a byte saying how many data sources there were in the
        // coming transaction stream response. For backwards compatibility we keep it as a byte and we introduce
        // the transaction obligation response type as -1
        byte responseType = dechunkingBuffer.readByte();
        if ( responseType == TransactionObligationResponse.RESPONSE_TYPE )
        {
            // It is a transaction obligation response
            long obligationTxId = dechunkingBuffer.readLong();
            return new TransactionObligationResponse<>( response, storeId, obligationTxId, channelReleaser );
        }

        // It's a transaction stream in this response
        TransactionStream transactions = visitor ->
        {
            NetworkReadableClosableChannel channel = new NetworkReadableClosableChannel( dechunkingBuffer );

            try ( PhysicalTransactionCursor<ReadableClosablePositionAwareChannel> cursor =
                          new PhysicalTransactionCursor<>( channel, entryReader ) )
            {
                while ( cursor.next() && !visitor.visit( cursor.get() ) )
                {
                }
            }
        };
        return new TransactionStreamResponse<>( response, storeId, transactions, channelReleaser );
    }

    protected abstract StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer );

    private void writeContext( RequestContext context, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeLong( context.getEpoch() );
        targetBuffer.writeInt( context.machineId() );
        targetBuffer.writeInt( context.getEventIdentifier() );
        long tx = context.lastAppliedTransaction();
        targetBuffer.writeLong( tx );
        targetBuffer.writeLong( context.getChecksum() );
    }

    public static class FileStreamsDeserializer210 implements Deserializer<Void>
    {
        private final StoreWriter writer;

        public FileStreamsDeserializer210( StoreWriter writer )
        {
            this.writer = writer;
        }

        // NOTICE: this assumes a "smart" ChannelBuffer that continues to next chunk
        @Override
        public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            int pathLength;
            while ( 0 != (pathLength = buffer.readUnsignedShort()) )
            {
                String path = readString( buffer, pathLength );
                boolean hasData = buffer.readByte() == 1;
                writer.write( path, hasData ? new BlockLogReader( buffer ) : null, temporaryBuffer, hasData, 1 );
            }
            writer.close();
            return null;
        }
    }

    public static class FileStreamsDeserializer310 implements Deserializer<Void>
    {
        private final StoreWriter writer;

        public FileStreamsDeserializer310( StoreWriter writer )
        {
            this.writer = writer;
        }

        // NOTICE: this assumes a "smart" ChannelBuffer that continues to next chunk
        @Override
        public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            int pathLength;
            while ( 0 != (pathLength = buffer.readUnsignedShort()) )
            {
                String path = readString( buffer, pathLength );
                boolean hasData = buffer.readByte() == 1;
                int recordSize = hasData ? buffer.readInt() : RecordFormat.NO_RECORD_SIZE;
                writer.write( path, hasData ? new BlockLogReader( buffer ) : null, temporaryBuffer, hasData,
                        recordSize );
            }
            writer.close();
            return null;
        }
    }

    public static class TransactionSerializer implements Serializer
    {
        private final TransactionRepresentation tx;

        public TransactionSerializer( TransactionRepresentation tx )
        {
            this.tx = tx;
        }

        @Override
        public void write( ChannelBuffer buffer ) throws IOException
        {
            NetworkFlushableChannel channel = new NetworkFlushableChannel( buffer );

            writeString( buffer, NeoStoreDataSource.DEFAULT_DATA_SOURCE_NAME );
            channel.putInt( tx.getAuthorId() );
            channel.putInt( tx.getMasterId() );
            channel.putLong( tx.getLatestCommittedTxWhenStarted() );
            channel.putLong( tx.getTimeStarted() );
            channel.putLong( tx.getTimeCommitted() );
            channel.putInt( tx.additionalHeader().length );
            channel.put( tx.additionalHeader(), tx.additionalHeader().length );
            new LogEntryWriter( channel ).serialize( tx );
        }
    }
}
