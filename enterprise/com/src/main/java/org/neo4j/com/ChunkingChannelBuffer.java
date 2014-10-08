/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * A decorator around a {@link ByteBuf} which adds the ability to transfer
 * chunks of it over a {@link Channel} when capacity is reached.
 * <p>
 * Instances of this class are created with an underlying buffer for holding
 * content, a capacity and a channel over which to stream the buffer contents
 * when that capacity is reached. When content addition would make the size of
 * the buffer exceed its capacity, a 2-byte continuation header is added in the
 * stream that contains flow information and protocol versions and it is
 * streamed over the channel. It is expected that a
 * {@link DechunkingChannelBuffer} sits on the other end waiting to deserialize
 * this stream. A final serialization round happens when <code>done()</code> is
 * called, if content has been added.
 * <p>
 * Each chunk written is marked as pending and no more than
 * MAX_WRITE_AHEAD_CHUNKS are left pending - in such a case the write process
 * sleeps until some acknowledgment comes back from the other side that chunks
 * have been read.
 */
public class ChunkingChannelBuffer extends ByteBuf implements ChannelFutureListener
{
    static final byte CONTINUATION_LAST = 0;
    static final byte CONTINUATION_MORE = 1;
    static final byte OUTCOME_SUCCESS = 0;
    static final byte OUTCOME_FAILURE = 1;
    private static final int MAX_WRITE_AHEAD_CHUNKS = 5;

    private ByteBuf buffer;
    private final Channel channel;
    private final int capacity;
    private int continuationPosition;
    private final AtomicInteger writeAheadCounter = new AtomicInteger();
    private volatile boolean failure;
    private final byte applicationProtocolVersion;
    private final byte internalProtocolVersion;

    public ChunkingChannelBuffer( ByteBuf buffer, Channel channel, int capacity,
            byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        this.buffer = buffer;
        this.channel = channel;
        this.capacity = capacity;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        addRoomForContinuationHeader();
    }

    private void addRoomForContinuationHeader()
    {
        continuationPosition = writerIndex();
        // byte 0: [pppp,ppoc] p: internal protocol version, o: outcome, c: continuation
        // byte 1: [aaaa,aaaa] a: application protocol version
        buffer.writeBytes( header( CONTINUATION_LAST ) );
    }

    private byte[] header( byte continuation )
    {
        byte[] header = new byte[2];
        header[0] = (byte)((internalProtocolVersion << 2) | ((failure?OUTCOME_FAILURE:OUTCOME_SUCCESS) << 1) | continuation );
        header[1] = applicationProtocolVersion;
        return header;
    }

    private void setContinuation( byte continuation )
    {
        buffer.setBytes( continuationPosition, header( continuation ) );
    }

    public void clear( boolean failure )
    {
        buffer.clear();
        this.failure = failure;
        addRoomForContinuationHeader();
    }

    @Override
    public ByteBuf clear()
    {
        clear( false );
        return this;
    }

    private void sendChunkIfNeeded( int bytesPlus )
    {
        // Note: This is wasteful, it should pack as much data as possible into the current chunk before sending it off.
        // Refactor when there is time.
        if ( writerIndex()+bytesPlus >= capacity )
        {
            setContinuation( CONTINUATION_MORE );
            writeCurrentChunk();
            buffer = channel.alloc().buffer();
            addRoomForContinuationHeader();
        }
    }

    private ChannelFuture writeCurrentChunk()
    {
        if ( !channel.isActive() )
            throw new ComException( "Channel has been closed, so no need to try to write to it anymore. Client closed it?" + channel.isActive() + ", " + channel.isOpen() + ", " + channel.isWritable() );

        waitForClientToCatchUpOnReadingChunks();
        writeAheadCounter.incrementAndGet();
        return channel.writeAndFlush( buffer ).addListener( this );
    }

    private void waitForClientToCatchUpOnReadingChunks()
    {
        // Wait until channel gets disconnected or client catches up.
        // If channel has been disconnected we can exit and the next write
        // will produce a decent exception out.
        boolean waited = false;
        while ( channel.isActive() && writeAheadCounter.get() >= MAX_WRITE_AHEAD_CHUNKS )
        {
            waited = true;
            try
            {
                Thread.sleep( 200 );
            }
            catch ( InterruptedException e )
            {   // OK
                Thread.interrupted();
            }
        }

        if ( waited && (!channel.isActive()) )
        {
            throw new ComException( "Channel has been closed" );
        }
    }

    @Override
    public void operationComplete( ChannelFuture future ) throws Exception
    {
        if ( !future.isDone() )
        {
            throw new ComException( "This should not be possible because we waited for the future to be done" );
        }

        if ( !future.isSuccess() || future.isCancelled() )
        {
            future.channel().close();
        }
        writeAheadCounter.decrementAndGet();
    }

    public ChannelFuture done()
    {
        if ( isReadable() /* Meaning that something has been written to it and can be read/sent */ )
        {
            return writeCurrentChunk();
        }
        else
        {
            return channel.newSucceededFuture();
        }
    }

    @Override
    public ChunkingChannelBuffer writeByte( int value )
    {
        sendChunkIfNeeded( 1 );
        buffer.writeByte( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeShort( int value )
    {
        sendChunkIfNeeded( 2 );
        buffer.writeShort( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeMedium( int value )
    {
        sendChunkIfNeeded( 4 );
        buffer.writeMedium( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeInt( int value )
    {
        sendChunkIfNeeded( 4 );
        buffer.writeInt( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeLong( long value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeLong( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeChar( int value )
    {
        sendChunkIfNeeded( 2 );
        buffer.writeChar( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeFloat( float value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeFloat( value );
        return this;
    }

    @Override
    public ChunkingChannelBuffer writeDouble( double value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeDouble( value );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src )
    {
        sendChunkIfNeeded( src.capacity() );
        buffer.writeBytes( src );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, length );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int srcIndex, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, srcIndex, length );
        return this;
    }

    @Override
    public ByteBuf writeBytes( byte[] src )
    {
        sendChunkIfNeeded( src.length );
        buffer.writeBytes( src );
        return this;
    }

    @Override
    public ByteBuf writeBytes( byte[] src, int srcIndex, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, srcIndex, length );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuffer src )
    {
        sendChunkIfNeeded( src.limit() );
        buffer.writeBytes( src );
        return this;
    }

    @Override
    public int writeBytes( InputStream in, int length ) throws IOException
    {
        sendChunkIfNeeded( length );
        return buffer.writeBytes( in, length );
    }

    @Override
    public int writeBytes( ScatteringByteChannel in, int length ) throws IOException
    {
        sendChunkIfNeeded( length );
        return buffer.writeBytes( in, length );
    }

    @Override
    public ByteBuf writeZero( int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeZero( length );
        return this;
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, byte value )
    {
        return buffer.indexOf( fromIndex, toIndex, value );
    }

    @Override
    public int bytesBefore( byte value )
    {
        return buffer.bytesBefore( value );
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        return buffer.bytesBefore( length, value );
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        return buffer.bytesBefore( index, length, value );
    }

    @Override
    public int forEachByte( ByteBufProcessor processor )
    {
        return buffer.forEachByte( processor );
    }

    @Override
    public int forEachByte( int index, int length, ByteBufProcessor processor )
    {
        return buffer.forEachByte( index, length, processor );
    }

    @Override
    public int forEachByteDesc( ByteBufProcessor processor )
    {
        return buffer.forEachByteDesc( processor );
    }

    @Override
    public int forEachByteDesc( int index, int length, ByteBufProcessor processor )
    {
        return buffer.forEachByteDesc( index, length, processor );
    }

    @Override
    public ByteBuf copy()
    {
        return buffer.copy();
    }

    @Override
    public ByteBuf copy( int index, int length )
    {
        return buffer.copy( index, length );
    }

    @Override
    public ByteBuf slice()
    {
        return buffer.slice();
    }

    @Override
    public ByteBuf slice( int index, int length )
    {
        return buffer.slice( index, length );
    }

    @Override
    public ByteBuf duplicate()
    {
        return buffer.duplicate();
    }

    @Override
    public int nioBufferCount()
    {
        return buffer.nioBufferCount();
    }

    @Override
    public ByteBuffer nioBuffer()
    {
        return buffer.nioBuffer();
    }

    @Override
    public ByteBuffer nioBuffer( int index, int length )
    {
        return buffer.nioBuffer( index, length );
    }

    @Override
    public ByteBuffer internalNioBuffer( int index, int length )
    {
        return buffer.internalNioBuffer( index, length );
    }

    @Override
    public ByteBuffer[] nioBuffers()
    {
        return buffer.nioBuffers();
    }

    @Override
    public ByteBuffer[] nioBuffers( int index, int length )
    {
        return buffer.nioBuffers( index, length );
    }

    @Override
    public boolean hasArray()
    {
        return buffer.hasArray();
    }

    @Override
    public byte[] array()
    {
        return buffer.array();
    }

    @Override
    public int arrayOffset()
    {
        return buffer.arrayOffset();
    }

    @Override
    public boolean hasMemoryAddress()
    {
        return buffer.hasMemoryAddress();
    }

    @Override
    public long memoryAddress()
    {
        return buffer.memoryAddress();
    }

    @Override
    public String toString( Charset charset )
    {
        return buffer.toString( charset );
    }

    @Override
    public String toString( int index, int length, Charset charset )
    {
        return buffer.toString( index, length, charset );
    }

    @Override
    public int hashCode()
    {
        return buffer.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        return buffer.equals( obj );
    }

    @Override
    public int compareTo( ByteBuf buffer )
    {
        return this.buffer.compareTo( buffer );
    }

    @Override
    public String toString()
    {
        return buffer.toString();
    }

    @Override
    public int capacity()
    {
        return buffer.capacity();
    }

    @Override
    public ByteBuf capacity( int newCapacity )
    {
        buffer.capacity( newCapacity );
        return this;
    }

    @Override
    public int maxCapacity()
    {
        return buffer.maxCapacity();
    }

    @Override
    public ByteBufAllocator alloc()
    {
        return buffer.alloc();
    }

    @Override
    public ByteOrder order()
    {
        return buffer.order();
    }

    @Override
    public ByteBuf order( ByteOrder endianness )
    {
        buffer.order( endianness );
        return this;
    }

    @Override
    public ByteBuf unwrap()
    {
        return buffer.unwrap();
    }

    @Override
    public boolean isDirect()
    {
        return buffer.isDirect();
    }

    @Override
    public int readerIndex()
    {
        return buffer.readerIndex();
    }

    @Override
    public ByteBuf readerIndex( int readerIndex )
    {
        buffer.readerIndex( readerIndex );
        return this;
    }

    @Override
    public int writerIndex()
    {
        return buffer.writerIndex();
    }

    @Override
    public ByteBuf writerIndex( int writerIndex )
    {
        buffer.writerIndex( writerIndex );
        return this;
    }

    @Override
    public ByteBuf setIndex( int readerIndex, int writerIndex )
    {
        buffer.setIndex( readerIndex, writerIndex );
        return this;
    }

    @Override
    public int readableBytes()
    {
        return buffer.readableBytes();
    }

    @Override
    public int writableBytes()
    {
        return buffer.writableBytes();
    }

    @Override
    public int maxWritableBytes()
    {
        return buffer.maxWritableBytes();
    }

    @Override
    public boolean isReadable()
    {
        return buffer.isReadable();
    }

    @Override
    public boolean isReadable( int size )
    {
        return buffer.isReadable( size );
    }

    @Override
    public boolean isWritable()
    {
        return buffer.isWritable();
    }

    @Override
    public boolean isWritable( int size )
    {
        return buffer.isWritable( size );
    }

    @Override
    public ByteBuf markReaderIndex()
    {
        buffer.markReaderIndex();
        return this;
    }

    @Override
    public ByteBuf resetReaderIndex()
    {
        buffer.resetReaderIndex();
        return this;
    }

    @Override
    public ByteBuf markWriterIndex()
    {
        buffer.markWriterIndex();
        return this;
    }

    @Override
    public ByteBuf resetWriterIndex()
    {
        buffer.resetWriterIndex();
        return this;
    }

    @Override
    public ByteBuf discardReadBytes()
    {
        buffer.discardReadBytes();
        return this;
    }

    @Override
    public ByteBuf discardSomeReadBytes()
    {
        buffer.discardSomeReadBytes();
        return this;
    }

    @Override
    public ByteBuf ensureWritable( int minWritableBytes )
    {
        buffer.ensureWritable( minWritableBytes );
        return this;
    }

    @Override
    public int ensureWritable( int minWritableBytes, boolean force )
    {
        return buffer.ensureWritable( minWritableBytes, force );
    }

    @Override
    public boolean getBoolean( int index )
    {
        return buffer.getBoolean( index );
    }

    @Override
    public byte getByte( int index )
    {
        return buffer.getByte( index );
    }

    @Override
    public short getUnsignedByte( int index )
    {
        return buffer.getUnsignedByte( index );
    }

    @Override
    public short getShort( int index )
    {
        return buffer.getShort( index );
    }

    @Override
    public int getUnsignedShort( int index )
    {
        return buffer.getUnsignedShort( index );
    }

    @Override
    public int getMedium( int index )
    {
        return buffer.getMedium( index );
    }

    @Override
    public int getUnsignedMedium( int index )
    {
        return buffer.getUnsignedMedium( index );
    }

    @Override
    public int getInt( int index )
    {
        return buffer.getInt( index );
    }

    @Override
    public long getUnsignedInt( int index )
    {
        return buffer.getUnsignedInt( index );
    }

    @Override
    public long getLong( int index )
    {
        return buffer.getLong( index );
    }

    @Override
    public char getChar( int index )
    {
        return buffer.getChar( index );
    }

    @Override
    public float getFloat( int index )
    {
        return buffer.getFloat( index );
    }

    @Override
    public double getDouble( int index )
    {
        return buffer.getDouble( index );
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst )
    {
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int length )
    {
        buffer.getBytes( index, dst, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int dstIndex, int length )
    {
        buffer.getBytes( index, dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst )
    {
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        buffer.getBytes( index, dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuffer dst )
    {
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, OutputStream out, int length ) throws IOException
    {
        buffer.getBytes( index, out, length );
        return this;
    }

    @Override
    public int getBytes( int index, GatheringByteChannel out, int length ) throws IOException
    {
        return buffer.getBytes( index, out, length );
    }

    @Override
    public ByteBuf setBoolean( int index, boolean value )
    {
        buffer.setBoolean( index, value );
        return this;
    }

    @Override
    public ByteBuf setByte( int index, int value )
    {
        buffer.setByte( index, value );
        return this;
    }

    @Override
    public ByteBuf setShort( int index, int value )
    {
        buffer.setShort( index, value );
        return this;
    }

    @Override
    public ByteBuf setMedium( int index, int value )
    {
        buffer.setMedium( index, value );
        return this;
    }

    @Override
    public ByteBuf setInt( int index, int value )
    {
        buffer.setInt( index, value );
        return this;
    }

    @Override
    public ByteBuf setLong( int index, long value )
    {
        buffer.setLong( index, value );
        return this;
    }

    @Override
    public ByteBuf setChar( int index, int value )
    {
        buffer.setChar( index, value );
        return this;
    }

    @Override
    public ByteBuf setFloat( int index, float value )
    {
        buffer.setFloat( index, value );
        return this;
    }

    @Override
    public ByteBuf setDouble( int index, double value )
    {
        buffer.setDouble( index, value );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src )
    {
        buffer.setBytes( index, src );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int length )
    {
        buffer.setBytes( index, src, length );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int srcIndex, int length )
    {
        buffer.setBytes( index, src, srcIndex, length );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src )
    {
        buffer.setBytes( index, src );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src, int srcIndex, int length )
    {
        buffer.setBytes( index, src, srcIndex, length );
        return this;
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuffer src )
    {
        buffer.setBytes( index, src );
        return this;
    }

    @Override
    public int setBytes( int index, InputStream in, int length ) throws IOException
    {
        return buffer.setBytes( index, in, length );
    }

    @Override
    public int setBytes( int index, ScatteringByteChannel in, int length ) throws IOException
    {
        return buffer.setBytes( index, in, length );
    }

    @Override
    public ByteBuf setZero( int index, int length )
    {
        buffer.setZero( index, length );
        return this;
    }

    @Override
    public boolean readBoolean()
    {
        return buffer.readBoolean();
    }

    @Override
    public byte readByte()
    {
        return buffer.readByte();
    }

    @Override
    public short readUnsignedByte()
    {
        return buffer.readUnsignedByte();
    }

    @Override
    public short readShort()
    {
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort()
    {
        return buffer.readUnsignedShort();
    }

    @Override
    public int readMedium()
    {
        return buffer.readMedium();
    }

    @Override
    public int readUnsignedMedium()
    {
        return buffer.readUnsignedMedium();
    }

    @Override
    public int readInt()
    {
        return buffer.readInt();
    }

    @Override
    public long readUnsignedInt()
    {
        return buffer.readUnsignedInt();
    }

    @Override
    public long readLong()
    {
        return buffer.readLong();
    }

    @Override
    public char readChar()
    {
        return buffer.readChar();
    }

    @Override
    public float readFloat()
    {
        return buffer.readFloat();
    }

    @Override
    public double readDouble()
    {
        return buffer.readDouble();
    }

    @Override
    public ByteBuf readBytes( int length )
    {
        buffer.readBytes( length );
        return this;
    }

    @Override
    public ByteBuf readSlice( int length )
    {
        buffer.readSlice( length );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst )
    {
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int length )
    {
        buffer.readBytes( dst, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int dstIndex, int length )
    {
        buffer.readBytes( dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( byte[] dst )
    {
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( byte[] dst, int dstIndex, int length )
    {
        buffer.readBytes( dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuffer dst )
    {
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( OutputStream out, int length ) throws IOException
    {
        buffer.readBytes( out, length );
        return this;
    }

    @Override
    public int readBytes( GatheringByteChannel out, int length ) throws IOException
    {
        return buffer.readBytes( out, length );
    }

    @Override
    public ByteBuf skipBytes( int length )
    {
        buffer.skipBytes( length );
        return this;
    }

    @Override
    public ByteBuf writeBoolean( boolean value )
    {
        return buffer.writeBoolean( value );
    }

    @Override
    public int refCnt()
    {
        throw new UnsupportedOperationException( "Reference counting not supported." );
    }

    @Override
    public boolean release()
    {
        throw new UnsupportedOperationException( "Reference counting not supported." );
    }

    @Override
    public boolean release( int decrement )
    {
        throw new UnsupportedOperationException( "Reference counting not supported." );
    }

    @Override
    public ByteBuf retain( int increment )
    {
        throw new UnsupportedOperationException( "Reference counting not supported." );
    }

    @Override
    public ByteBuf retain()
    {
        throw new UnsupportedOperationException( "Reference counting not supported." );
    }
}
