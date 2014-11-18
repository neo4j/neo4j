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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static org.neo4j.kernel.impl.util.Bits.numbersToBitString;

public class DechunkingChannelBuffer extends ByteBuf
{
    private final BlockingReadHandler<ByteBuf> reader;
    private ByteBuf buffer;
    private boolean more;
    private boolean hasMarkedReaderIndex;
    private final long timeoutMillis;
    private boolean failure;
    private final byte applicationProtocolVersion;
    private final byte internalProtocolVersion;

    DechunkingChannelBuffer( BlockingReadHandler<ByteBuf> reader, long timeoutMillis, byte internalProtocolVersion,
            byte applicationProtocolVersion )
    {
        this.reader = reader;
        this.timeoutMillis = timeoutMillis;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        readNextChunk();
    }

    protected ByteBuf readNext()
    {
        try
        {
            ByteBuf result = reader.read( timeoutMillis, TimeUnit.MILLISECONDS );
            if ( result == null )
            {
                throw new ComException( "Channel has been closed" );
            }
            return result;
        }
        catch ( IOException | BlockingReadTimeoutException | InterruptedException e )
        {
            throw new ComException( e );
        }
    }

    private void readNextChunkIfNeeded( int bytesPlus )
    {
        if ( buffer.readableBytes() < bytesPlus && more )
        {
            readNextChunk();
        }
    }

    private void readNextChunk()
    {
        ByteBuf readBuffer = readNext();

        /* Header layout:
         * [    ,    ][    ,   x] 0: last chunk in message, 1: there a more chunks after this one
         * [    ,    ][    ,  x ] 0: success, 1: failure
         * [    ,    ][xxxx,xx  ] internal protocol version
         * [xxxx,xxxx][    ,    ] application protocol version */
        byte[] header = new byte[2];
        readBuffer.readBytes( header );
        more = (header[0] & 0x1) != 0;
        failure = (header[0] & 0x2) != 0;
        assertSameProtocolVersion( header, internalProtocolVersion, applicationProtocolVersion );

        if ( !more && buffer == null )
        {
            // Optimization: this is the first chunk and it'll be the only chunk
            // in this message.
            buffer = readBuffer;
        }
        else
        {
            buffer = buffer == null ? Unpooled.buffer() : buffer;
            discardReadBytes();
            buffer.writeBytes( readBuffer );
            readBuffer.release();
        }

        if ( failure )
        {
            readAndThrowFailureResponse();
        }
    }

    static void assertSameProtocolVersion( byte[] header, byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        /* [aaaa,aaaa][pppp,ppoc]
         * Only 6 bits for internal protocol version, yielding 64 values. It's ok to wrap around because
         * It's highly unlikely that instances that are so far apart in versions will communicate
         * with each other.
         */
        byte readInternalProtocolVersion = (byte) ((header[0] & 0x7C) >>> 2);
        if ( readInternalProtocolVersion != internalProtocolVersion )
        {
            throw new IllegalProtocolVersionException( internalProtocolVersion, readInternalProtocolVersion,
                    "Unexpected internal protocol version " + readInternalProtocolVersion +
                    ", expected " + internalProtocolVersion + ". Header:" + numbersToBitString( header ) );
        }
        if ( header[1] != applicationProtocolVersion )
        {
            throw new IllegalProtocolVersionException( applicationProtocolVersion, header[1],
                    "Unexpected application protocol version " + header[1] +
                    ", expected " + applicationProtocolVersion + ". Header:" + numbersToBitString( header ) );
        }
    }

    private void readAndThrowFailureResponse()
    {
        Throwable cause;
        try
        {
            ObjectInputStream input = new ObjectInputStream( asInputStream() );
            cause = (Throwable) input.readObject();
        }
        catch ( Throwable e )
        {
            // Note: this is due to a problem with the streaming of exceptions, the ChunkingChannelBuffer will almost
            // always sends exceptions back as two chunks, the first one empty and the second with the exception.
            // We hit this when we try to read the exception of the first one, and in reading it hit the second
            // chunk with the "real" exception. This should be revisited to 1) clear up the chunking and 2) handle
            // serialized exceptions spanning multiple chunks.
            if ( e instanceof RuntimeException ) throw (RuntimeException) e;
            if ( e instanceof Error ) throw (Error) e;

            throw new ComException( e );
        }

        if ( cause instanceof RuntimeException ) throw (RuntimeException) cause;
        if ( cause instanceof Error ) throw (Error) cause;
        throw new ComException( cause );
    }

    public boolean failure()
    {
        return failure;
    }

    /**
     * Will return the capacity of the current chunk only
     */
    @Override
    public int capacity()
    {
        return buffer.capacity();
    }

    @Override
    public ByteBuf capacity( int newCapacity )
    {
        buffer.capacity(newCapacity);
        return this;
    }

    @Override
    public int maxCapacity()
    {
        return buffer.capacity();
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
        buffer.order(endianness);
        return this;
    }

    @Override
    public ByteBuf unwrap()
    {
        return buffer;
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

    /**
     * Will return amount of readable bytes in this chunk only
     */
    @Override
    public int readableBytes()
    {
        return buffer.readableBytes();
    }

    @Override
    public int writableBytes()
    {
        return 0;
    }

    @Override
    public int maxWritableBytes()
    {
        return 0;
    }

    /**
     * Can fetch the next chunk if needed
     */
    @Override
    public boolean isReadable()
    {
        readNextChunkIfNeeded( 1 );
        return buffer.isReadable();
    }

    @Override
    public boolean isReadable( int size )
    {
        return false;
    }

    @Override
    public boolean isWritable()
    {
        return buffer.isWritable();
    }

    @Override
    public boolean isWritable( int size )
    {
        return false;
    }

    @Override
    public ByteBuf clear()
    {
        buffer.clear();
        return this;
    }

    @Override
    public ByteBuf markReaderIndex()
    {
        buffer.markReaderIndex();
        hasMarkedReaderIndex = true;
        return this;
    }

    @Override
    public ByteBuf resetReaderIndex()
    {
        buffer.resetReaderIndex();
        hasMarkedReaderIndex = false;
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
        int oldReaderIndex = buffer.readerIndex();
        if ( hasMarkedReaderIndex )
        {
            buffer.resetReaderIndex();
        }
        int bytesToDiscard = buffer.readerIndex();
        buffer.discardReadBytes();
        if ( hasMarkedReaderIndex )
        {
            buffer.readerIndex( oldReaderIndex-bytesToDiscard );
        }
        return this;
    }

    @Override
    public ByteBuf discardSomeReadBytes()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf ensureWritable( int minWritableBytes )
    {
        throw unsupportedOperation();
    }

    @Override
    public int ensureWritable( int minWritableBytes, boolean force )
    {
        throw unsupportedOperation();
    }

    @Override
    public boolean getBoolean( int index )
    {
        throw unsupportedOperation();
    }

    @Override
    public byte getByte( int index )
    {
        readNextChunkIfNeeded( 1 );
        return buffer.getByte( index );
    }

    @Override
    public short getUnsignedByte( int index )
    {
        readNextChunkIfNeeded( 1 );
        return buffer.getUnsignedByte( index );
    }

    @Override
    public short getShort( int index )
    {
        readNextChunkIfNeeded( 2 );
        return buffer.getShort( index );
    }

    @Override
    public int getUnsignedShort( int index )
    {
        readNextChunkIfNeeded( 2 );
        return buffer.getUnsignedShort( index );
    }

    @Override
    public int getMedium( int index )
    {
        readNextChunkIfNeeded( 4 );
        return buffer.getMedium( index );
    }

    @Override
    public int getUnsignedMedium( int index )
    {
        readNextChunkIfNeeded( 4 );
        return buffer.getUnsignedMedium( index );
    }

    @Override
    public int getInt( int index )
    {
        readNextChunkIfNeeded( 4 );
        return buffer.getInt( index );
    }

    @Override
    public long getUnsignedInt( int index )
    {
        readNextChunkIfNeeded( 4 );
        return buffer.getUnsignedInt( index );
    }

    @Override
    public long getLong( int index )
    {
        readNextChunkIfNeeded( 8 );
        return buffer.getLong( index );
    }

    @Override
    public char getChar( int index )
    {
        readNextChunkIfNeeded( 2 );
        return buffer.getChar( index );
    }

    @Override
    public float getFloat( int index )
    {
        readNextChunkIfNeeded( 8 );
        return buffer.getFloat( index );
    }

    @Override
    public double getDouble( int index )
    {
        readNextChunkIfNeeded( 8 );
        return buffer.getDouble( index );
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.writableBytes() );
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int dstIndex, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.length );
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuffer dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.limit() );
        buffer.getBytes( index, dst );
        return this;
    }

    @Override
    public ByteBuf getBytes( int index, OutputStream out, int length ) throws IOException
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, out, length );
        return this;
    }

    @Override
    public int getBytes( int index, GatheringByteChannel out, int length ) throws IOException
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        return buffer.getBytes( index, out, length );
    }

    @Override
    public ByteBuf setBoolean( int index, boolean value )
    {
        return null;
    }

    private UnsupportedOperationException unsupportedOperation()
    {
        return new UnsupportedOperationException( "Not supported in a DechunkingChannelBuffer, it's used merely for reading" );
    }

    @Override
    public ByteBuf setByte( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setShort( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setMedium( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setInt( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setLong( int index, long value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setChar( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setFloat( int index, float value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setDouble( int index, double value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public int setBytes( int index, InputStream in, int length ) throws IOException
    {
        throw unsupportedOperation();
    }

    @Override
    public int setBytes( int index, ScatteringByteChannel in, int length ) throws IOException
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf setZero( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public boolean readBoolean()
    {
        return buffer.readBoolean();
    }

    @Override
    public byte readByte()
    {
        readNextChunkIfNeeded( 1 );
        return buffer.readByte();
    }

    @Override
    public short readUnsignedByte()
    {
        readNextChunkIfNeeded( 1 );
        return buffer.readUnsignedByte();
    }

    @Override
    public short readShort()
    {
        readNextChunkIfNeeded( 2 );
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort()
    {
        readNextChunkIfNeeded( 2 );
        return buffer.readUnsignedShort();
    }

    @Override
    public int readMedium()
    {
        readNextChunkIfNeeded( 4 );
        return buffer.readMedium();
    }

    @Override
    public int readUnsignedMedium()
    {
        readNextChunkIfNeeded( 4 );
        return buffer.readUnsignedMedium();
    }

    @Override
    public int readInt()
    {
        readNextChunkIfNeeded( 4 );
        return buffer.readInt();
    }

    @Override
    public long readUnsignedInt()
    {
        readNextChunkIfNeeded( 4 );
        return buffer.readUnsignedInt();
    }

    @Override
    public long readLong()
    {
        readNextChunkIfNeeded( 8 );
        return buffer.readLong();
    }

    @Override
    public char readChar()
    {
        readNextChunkIfNeeded( 2 );
        return buffer.readChar();
    }

    @Override
    public float readFloat()
    {
        readNextChunkIfNeeded( 8 );
        return buffer.readFloat();
    }

    @Override
    public double readDouble()
    {
        readNextChunkIfNeeded( 8 );
        return buffer.readDouble();
    }

    @Override
    public ByteBuf readBytes( int length )
    {
        readNextChunkIfNeeded( length );
        return buffer.readBytes( length );
    }

    @Override
    public ByteBuf readSlice( int length )
    {
        readNextChunkIfNeeded( length );
        return buffer.readSlice( length );
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst )
    {
        readNextChunkIfNeeded( dst.writableBytes() );
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int dstIndex, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( byte[] dst )
    {
        readNextChunkIfNeeded( dst.length );
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( byte[] dst, int dstIndex, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, dstIndex, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuffer dst )
    {
        readNextChunkIfNeeded( dst.limit() );
        buffer.readBytes( dst );
        return this;
    }

    @Override
    public ByteBuf readBytes( OutputStream out, int length ) throws IOException
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( out, length );
        return this;
    }

    @Override
    public int readBytes( GatheringByteChannel out, int length ) throws IOException
    {
        readNextChunkIfNeeded( length );
        return buffer.readBytes( out, length );
    }

    @Override
    public ByteBuf skipBytes( int length )
    {
        readNextChunkIfNeeded( length );
        buffer.skipBytes( length );
        return this;
    }

    @Override
    public ByteBuf writeBoolean( boolean value )
    {
        return null;
    }

    @Override
    public ByteBuf writeByte( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeShort( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeMedium( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeInt( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeLong( long value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeChar( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeFloat( float value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeDouble( double value )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( byte[] src )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( byte[] src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeBytes( ByteBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public int writeBytes( InputStream in, int length ) throws IOException
    {
        throw unsupportedOperation();
    }

    @Override
    public int writeBytes( ScatteringByteChannel in, int length ) throws IOException
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf writeZero( int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int forEachByte( ByteBufProcessor processor )
    {
        return 0;
    }

    @Override
    public int forEachByte( int index, int length, ByteBufProcessor processor )
    {
        return 0;
    }

    @Override
    public int forEachByteDesc( ByteBufProcessor processor )
    {
        return 0;
    }

    @Override
    public int forEachByteDesc( int index, int length, ByteBufProcessor processor )
    {
        return 0;
    }

    @Override
    public ByteBuf copy()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf copy( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf slice()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf slice( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf duplicate()
    {
        throw unsupportedOperation();
    }

    @Override
    public int nioBufferCount()
    {
        return 0;
    }

    @Override
    public ByteBuffer nioBuffer()
    {
        return null;
    }

    @Override
    public ByteBuffer nioBuffer( int index, int length )
    {
        return null;
    }

    @Override
    public ByteBuffer internalNioBuffer( int index, int length )
    {
        return null;
    }

    @Override
    public ByteBuffer[] nioBuffers()
    {
        return new ByteBuffer[0];
    }

    @Override
    public ByteBuffer[] nioBuffers( int index, int length )
    {
        return new ByteBuffer[0];
    }

    @Override
    public boolean hasArray()
    {
        throw unsupportedOperation();
    }

    @Override
    public byte[] array()
    {
        throw unsupportedOperation();
    }

    @Override
    public int arrayOffset()
    {
        throw unsupportedOperation();
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
    public ByteBuf retain( int increment )
    {
        throw unsupportedOperation();
    }

    @Override
    public boolean release()
    {
        throw unsupportedOperation();
    }

    @Override
    public boolean release( int decrement )
    {
        throw unsupportedOperation();
    }

    @Override
    public int refCnt()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuf retain()
    {
        throw unsupportedOperation();
    }

    private InputStream asInputStream()
    {
        return new InputStream()
        {
            @Override
            public int read( byte[] b ) throws IOException
            {
                readBytes( b );
                return b.length;
            }

            @Override
            public int read( byte[] b, int off, int len ) throws IOException
            {
                readBytes( b, off, len );
                return len;
            }

            @Override
            public long skip( long n ) throws IOException
            {
                skipBytes( (int)n );
                return n;
            }

            @Override
            public int available() throws IOException
            {
                return super.available();
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public synchronized void mark( int readlimit )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public synchronized void reset() throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean markSupported()
            {
                return false;
            }

            @Override
            public int read() throws IOException
            {
                return readByte();
            }
        };
    }
}