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
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A decorator around a {@link ChannelBuffer} which adds the ability to transfer
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
public class ChunkingChannelBuffer implements ChannelBuffer, ChannelFutureListener
{
    static final byte CONTINUATION_LAST = 0;
    static final byte CONTINUATION_MORE = 1;
    static final byte OUTCOME_SUCCESS = 0;
    static final byte OUTCOME_FAILURE = 1;

    protected static final int MAX_WRITE_AHEAD_CHUNKS = 5;

    private ChannelBuffer buffer;
    private final Channel channel;
    private final int capacity;
    private int continuationPosition;
    private final AtomicInteger writeAheadCounter = new AtomicInteger();
    private volatile boolean failure;
    private final byte applicationProtocolVersion;
    private final byte internalProtocolVersion;

    public ChunkingChannelBuffer( ChannelBuffer buffer, Channel channel, int capacity,
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
        header[0] = (byte)((internalProtocolVersion << 2) | ((failure ? OUTCOME_FAILURE : OUTCOME_SUCCESS) << 1) |
                continuation );
        header[1] = applicationProtocolVersion;
        return header;
    }

    private void setContinuation( byte continuation )
    {
        buffer.setBytes( continuationPosition, header( continuation ) );
    }

    @Override
    public ChannelBufferFactory factory()
    {
        return buffer.factory();
    }

    @Override
    public int capacity()
    {
        return buffer.capacity();
    }

    @Override
    public ByteOrder order()
    {
        return buffer.order();
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
    public void readerIndex( int readerIndex )
    {
        buffer.readerIndex( readerIndex );
    }

    @Override
    public int writerIndex()
    {
        return buffer.writerIndex();
    }

    @Override
    public void writerIndex( int writerIndex )
    {
        buffer.writerIndex( writerIndex );
    }

    @Override
    public void setIndex( int readerIndex, int writerIndex )
    {
        buffer.setIndex( readerIndex, writerIndex );
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
    public boolean readable()
    {
        return buffer.readable();
    }

    @Override
    public boolean writable()
    {
        return buffer.writable();
    }

    public void clear( boolean failure )
    {
        buffer.clear();
        this.failure = failure;
        addRoomForContinuationHeader();
    }

    @Override
    public void clear()
    {
        clear( false );
    }

    @Override
    public void markReaderIndex()
    {
        buffer.markReaderIndex();
    }

    @Override
    public void resetReaderIndex()
    {
        buffer.resetReaderIndex();
    }

    @Override
    public void markWriterIndex()
    {
        buffer.markWriterIndex();
    }

    @Override
    public void resetWriterIndex()
    {
        buffer.resetWriterIndex();
    }

    @Override
    public void discardReadBytes()
    {
        buffer.discardReadBytes();
    }

    @Override
    public void ensureWritableBytes( int writableBytes )
    {
        buffer.ensureWritableBytes( writableBytes );
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
    public void getBytes( int index, ChannelBuffer dst )
    {
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int length )
    {
        buffer.getBytes( index, dst, length );
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int dstIndex, int length )
    {
        buffer.getBytes( index, dst, dstIndex, length );
    }

    @Override
    public void getBytes( int index, byte[] dst )
    {
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        buffer.getBytes( index, dst, dstIndex, length );
    }

    @Override
    public void getBytes( int index, ByteBuffer dst )
    {
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, OutputStream out, int length ) throws IOException
    {
        buffer.getBytes( index, out, length );
    }

    @Override
    public int getBytes( int index, GatheringByteChannel out, int length ) throws IOException
    {
        return buffer.getBytes( index, out, length );
    }

    @Override
    public void setByte( int index, int value )
    {
        buffer.setByte( index, value );
    }

    @Override
    public void setShort( int index, int value )
    {
        buffer.setShort( index, value );
    }

    @Override
    public void setMedium( int index, int value )
    {
        buffer.setMedium( index, value );
    }

    @Override
    public void setInt( int index, int value )
    {
        buffer.setInt( index, value );
    }

    @Override
    public void setLong( int index, long value )
    {
        buffer.setLong( index, value );
    }

    @Override
    public void setChar( int index, int value )
    {
        buffer.setChar( index, value );
    }

    @Override
    public void setFloat( int index, float value )
    {
        buffer.setFloat( index, value );
    }

    @Override
    public void setDouble( int index, double value )
    {
        buffer.setDouble( index, value );
    }

    @Override
    public void setBytes( int index, ChannelBuffer src )
    {
        buffer.setBytes( index, src );
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int length )
    {
        buffer.setBytes( index, src, length );
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int srcIndex, int length )
    {
        buffer.setBytes( index, src, srcIndex, length );
    }

    @Override
    public void setBytes( int index, byte[] src )
    {
        buffer.setBytes( index, src );
    }

    @Override
    public void setBytes( int index, byte[] src, int srcIndex, int length )
    {
        buffer.setBytes( index, src, srcIndex, length );
    }

    @Override
    public void setBytes( int index, ByteBuffer src )
    {
        buffer.setBytes( index, src );
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
    public void setZero( int index, int length )
    {
        buffer.setZero( index, length );
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
    public ChannelBuffer readBytes( int length )
    {
        return buffer.readBytes( length );
    }

    @Override
    public ChannelBuffer readBytes( ChannelBufferIndexFinder indexFinder )
    {
        return buffer.readBytes( indexFinder );
    }

    @Override
    public ChannelBuffer readSlice( int length )
    {
        return buffer.readSlice( length );
    }

    @Override
    public ChannelBuffer readSlice( ChannelBufferIndexFinder indexFinder )
    {
        return buffer.readSlice( indexFinder );
    }

    @Override
    public void readBytes( ChannelBuffer dst )
    {
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( ChannelBuffer dst, int length )
    {
        buffer.readBytes( dst, length );
    }

    @Override
    public void readBytes( ChannelBuffer dst, int dstIndex, int length )
    {
        buffer.readBytes( dst, dstIndex, length );
    }

    @Override
    public void readBytes( byte[] dst )
    {
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( byte[] dst, int dstIndex, int length )
    {
        buffer.readBytes( dst, dstIndex, length );
    }

    @Override
    public void readBytes( ByteBuffer dst )
    {
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( OutputStream out, int length ) throws IOException
    {
        buffer.readBytes( out, length );
    }

    @Override
    public int readBytes( GatheringByteChannel out, int length ) throws IOException
    {
        return buffer.readBytes( out, length );
    }

    @Override
    public void skipBytes( int length )
    {
        buffer.skipBytes( length );
    }

    @Override
    public int skipBytes( ChannelBufferIndexFinder indexFinder )
    {
        return buffer.skipBytes( indexFinder );
    }

    private void sendChunkIfNeeded( int bytesPlus )
    {
        // Note: This is wasteful, it should pack as much data as possible into the current chunk before sending it off.
        // Refactor when there is time.
        if ( writerIndex() + bytesPlus >= capacity )
        {
            setContinuation( CONTINUATION_MORE );
            writeCurrentChunk();
            buffer = newChannelBuffer();
            addRoomForContinuationHeader();
        }
    }

    protected ChannelBuffer newChannelBuffer()
    {
        return ChannelBuffers.dynamicBuffer( capacity );
    }

    private void writeCurrentChunk()
    {
        if ( !channel.isOpen() || !channel.isConnected() || !channel.isBound() )
        {
            throw new ComException( "Channel has been closed, so no need to try to write to it anymore. Client closed it?" );
        }

        waitForClientToCatchUpOnReadingChunks();
        ChannelFuture future = channel.write( buffer );
        future.addListener( newChannelFutureListener( buffer ) );
        writeAheadCounter.incrementAndGet();
    }

    protected ChannelFutureListener newChannelFutureListener( ChannelBuffer buffer )
    {
        return this;
    }

    private void waitForClientToCatchUpOnReadingChunks()
    {
        // Wait until channel gets disconnected or client catches up.
        // If channel has been disconnected we can exit and the next write
        // will produce a decent exception out.
        boolean waited = false;
        while ( channel.isConnected() && writeAheadCounter.get() >= MAX_WRITE_AHEAD_CHUNKS )
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

        if ( waited && (!channel.isConnected() || !channel.isOpen()) )
        {
            throw new ComException( "Channel has been closed" );
        }
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        if ( !future.isDone() )
        {
            throw new ComException( "This should not be possible because we waited for the future to be done" );
        }

        if ( !future.isSuccess() || future.isCancelled() )
        {
            future.getChannel().close();
        }
        writeAheadCounter.decrementAndGet();
    }

    public void done()
    {
        if ( readable() /* Meaning that something has been written to it and can be read/sent */ )
        {
            writeCurrentChunk();
        }
    }

    @Override
    public void writeByte( int value )
    {
        sendChunkIfNeeded( 1 );
        buffer.writeByte( value );
    }

    @Override
    public void writeShort( int value )
    {
        sendChunkIfNeeded( 2 );
        buffer.writeShort( value );
    }

    @Override
    public void writeMedium( int value )
    {
        sendChunkIfNeeded( 4 );
        buffer.writeMedium( value );
    }

    @Override
    public void writeInt( int value )
    {
        sendChunkIfNeeded( 4 );
        buffer.writeInt( value );
    }

    @Override
    public void writeLong( long value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeLong( value );
    }

    @Override
    public void writeChar( int value )
    {
        sendChunkIfNeeded( 2 );
        buffer.writeChar( value );
    }

    @Override
    public void writeFloat( float value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeFloat( value );
    }

    @Override
    public void writeDouble( double value )
    {
        sendChunkIfNeeded( 8 );
        buffer.writeDouble( value );
    }

    @Override
    public void writeBytes( ChannelBuffer src )
    {
        sendChunkIfNeeded( src.capacity() );
        buffer.writeBytes( src );
    }

    @Override
    public void writeBytes( ChannelBuffer src, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, length );
    }

    @Override
    public void writeBytes( ChannelBuffer src, int srcIndex, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, srcIndex, length );
    }

    @Override
    public void writeBytes( byte[] src )
    {
        sendChunkIfNeeded( src.length );
        buffer.writeBytes( src );
    }

    @Override
    public void writeBytes( byte[] src, int srcIndex, int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeBytes( src, srcIndex, length );
    }

    @Override
    public void writeBytes( ByteBuffer src )
    {
        sendChunkIfNeeded( src.limit() );
        buffer.writeBytes( src );
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
    public void writeZero( int length )
    {
        sendChunkIfNeeded( length );
        buffer.writeZero( length );
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, byte value )
    {
        return buffer.indexOf( fromIndex, toIndex, value );
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, ChannelBufferIndexFinder indexFinder )
    {
        return buffer.indexOf( fromIndex, toIndex, indexFinder );
    }

    @Override
    public int bytesBefore( byte value )
    {
        return buffer.bytesBefore( value );
    }

    @Override
    public int bytesBefore( ChannelBufferIndexFinder indexFinder )
    {
        return buffer.bytesBefore( indexFinder );
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        return buffer.bytesBefore( length, value );
    }

    @Override
    public int bytesBefore( int length, ChannelBufferIndexFinder indexFinder )
    {
        return buffer.bytesBefore( length, indexFinder );
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        return buffer.bytesBefore( index, length, value );
    }

    @Override
    public int bytesBefore( int index, int length, ChannelBufferIndexFinder indexFinder )
    {
        return buffer.bytesBefore( index, length, indexFinder );
    }

    @Override
    public ChannelBuffer copy()
    {
        return buffer.copy();
    }

    @Override
    public ChannelBuffer copy( int index, int length )
    {
        return buffer.copy( index, length );
    }

    @Override
    public ChannelBuffer slice()
    {
        return buffer.slice();
    }

    @Override
    public ChannelBuffer slice( int index, int length )
    {
        return buffer.slice( index, length );
    }

    @Override
    public ChannelBuffer duplicate()
    {
        return buffer.duplicate();
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return buffer.toByteBuffer();
    }

    @Override
    public ByteBuffer toByteBuffer( int index, int length )
    {
        return buffer.toByteBuffer( index, length );
    }

    @Override
    public ByteBuffer[] toByteBuffers()
    {
        return buffer.toByteBuffers();
    }

    @Override
    public ByteBuffer[] toByteBuffers( int index, int length )
    {
        return buffer.toByteBuffers( index, length );
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
    public String toString( String charsetName )
    {
        return buffer.toString( charsetName );
    }

    @Override
    public String toString( String charsetName, ChannelBufferIndexFinder terminatorFinder )
    {
        return buffer.toString( charsetName, terminatorFinder );
    }

    @Override
    public String toString( int index, int length, String charsetName )
    {
        return buffer.toString( index, length, charsetName );
    }

    @Override
    public String toString( int index, int length, String charsetName,
            ChannelBufferIndexFinder terminatorFinder )
    {
        return buffer.toString( index, length, charsetName, terminatorFinder );
    }

    @Override
    public int compareTo( ChannelBuffer buffer )
    {
        return this.buffer.compareTo( buffer );
    }

    @Override
    public String toString()
    {
        return buffer.toString();
    }
}
