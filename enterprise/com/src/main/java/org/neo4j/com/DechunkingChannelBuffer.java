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
import org.jboss.netty.handler.queue.BlockingReadHandler;

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

class DechunkingChannelBuffer implements ChannelBuffer
{
    private final BlockingReadHandler<ChannelBuffer> reader;
    private ChannelBuffer buffer;
    private boolean more;
    private boolean hasMarkedReaderIndex;
    private final long timeoutMillis;
    private boolean failure;
    private final byte applicationProtocolVersion;
    private final byte internalProtocolVersion;

    DechunkingChannelBuffer( BlockingReadHandler<ChannelBuffer> reader, long timeoutMillis, byte internalProtocolVersion,
            byte applicationProtocolVersion )
    {
        this.reader = reader;
        this.timeoutMillis = timeoutMillis;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        readNextChunk();
    }

    private ChannelBuffer readNext()
    {
        try
        {
            ChannelBuffer result = reader.read( timeoutMillis, TimeUnit.MILLISECONDS );
            if ( result == null )
            {
                throw new ComException( "Channel has been closed" );
            }
            return result;
        }
        catch ( IOException | InterruptedException e )
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
        ChannelBuffer readBuffer = readNext();

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
            buffer = buffer == null ? ChannelBuffers.dynamicBuffer() : buffer;
            discardReadBytes();
            buffer.writeBytes( readBuffer );
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
            try ( ObjectInputStream input = new ObjectInputStream( asInputStream() ) )
            {
                cause = (Throwable) input.readObject();
            }
        }
        catch ( Throwable e )
        {
            // Note: this is due to a problem with the streaming of exceptions, the ChunkingChannelBuffer will almost
            // always sends exceptions back as two chunks, the first one empty and the second with the exception.
            // We hit this when we try to read the exception of the first one, and in reading it hit the second
            // chunk with the "real" exception. This should be revisited to 1) clear up the chunking and 2) handle
            // serialized exceptions spanning multiple chunks.
            if ( e instanceof RuntimeException )
            {
                throw (RuntimeException) e;
            }
            if ( e instanceof Error )
            {
                throw (Error) e;
            }

            throw new ComException( e );
        }

        if ( cause instanceof RuntimeException )
        {
            throw (RuntimeException) cause;
        }
        if ( cause instanceof Error )
        {
            throw (Error) cause;
        }
        throw new ComException( cause );
    }

    @Override
    public ChannelBufferFactory factory()
    {
        return buffer.factory();
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

    /**
     * Can fetch the next chunk if needed
     */
    @Override
    public boolean readable()
    {
        readNextChunkIfNeeded( 1 );
        return buffer.readable();
    }

    @Override
    public boolean writable()
    {
        return buffer.writable();
    }

    @Override
    public void clear()
    {
        buffer.clear();
    }

    @Override
    public void markReaderIndex()
    {
        buffer.markReaderIndex();
        hasMarkedReaderIndex = true;
    }

    @Override
    public void resetReaderIndex()
    {
        buffer.resetReaderIndex();
        hasMarkedReaderIndex = false;
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
        int oldReaderIndex = buffer.readerIndex();
        if ( hasMarkedReaderIndex )
        {
            buffer.resetReaderIndex();
        }
        int bytesToDiscard = buffer.readerIndex();
        buffer.discardReadBytes();
        if ( hasMarkedReaderIndex )
        {
            buffer.readerIndex( oldReaderIndex - bytesToDiscard );
        }
    }

    @Override
    public void ensureWritableBytes( int writableBytes )
    {
        buffer.ensureWritableBytes( writableBytes );
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
    public void getBytes( int index, ChannelBuffer dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.writableBytes() );
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, length );
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int dstIndex, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, dstIndex, length );
    }

    @Override
    public void getBytes( int index, byte[] dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.length );
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, dst, dstIndex, length );
    }

    @Override
    public void getBytes( int index, ByteBuffer dst )
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( dst.limit() );
        buffer.getBytes( index, dst );
    }

    @Override
    public void getBytes( int index, OutputStream out, int length ) throws IOException
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        buffer.getBytes( index, out, length );
    }

    @Override
    public int getBytes( int index, GatheringByteChannel out, int length ) throws IOException
    {
        // TODO We need a loop for this (if dst is bigger than chunk size)
        readNextChunkIfNeeded( length );
        return buffer.getBytes( index, out, length );
    }

    private UnsupportedOperationException unsupportedOperation()
    {
        return new UnsupportedOperationException( "Not supported in a DechunkingChannelBuffer, it's used merely for reading" );
    }

    @Override
    public void setByte( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setShort( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setMedium( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setInt( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setLong( int index, long value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setChar( int index, int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setFloat( int index, float value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setDouble( int index, double value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, ChannelBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, byte[] src )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, byte[] src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setBytes( int index, ByteBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public int setBytes( int index, InputStream in, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public int setBytes( int index, ScatteringByteChannel in, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void setZero( int index, int length )
    {
        throw unsupportedOperation();
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
    public ChannelBuffer readBytes( int length )
    {
        readNextChunkIfNeeded( length );
        return buffer.readBytes( length );
    }

    @Override
    public ChannelBuffer readBytes( ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer readSlice( int length )
    {
        readNextChunkIfNeeded( length );
        return buffer.readSlice( length );
    }

    @Override
    public ChannelBuffer readSlice( ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public void readBytes( ChannelBuffer dst )
    {
        readNextChunkIfNeeded( dst.writableBytes() );
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( ChannelBuffer dst, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, length );
    }

    @Override
    public void readBytes( ChannelBuffer dst, int dstIndex, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, dstIndex, length );
    }

    @Override
    public void readBytes( byte[] dst )
    {
        readNextChunkIfNeeded( dst.length );
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( byte[] dst, int dstIndex, int length )
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( dst, dstIndex, length );
    }

    @Override
    public void readBytes( ByteBuffer dst )
    {
        readNextChunkIfNeeded( dst.limit() );
        buffer.readBytes( dst );
    }

    @Override
    public void readBytes( OutputStream out, int length ) throws IOException
    {
        readNextChunkIfNeeded( length );
        buffer.readBytes( out, length );
    }

    @Override
    public int readBytes( GatheringByteChannel out, int length ) throws IOException
    {
        readNextChunkIfNeeded( length );
        return buffer.readBytes( out, length );
    }

    @Override
    public void skipBytes( int length )
    {
        readNextChunkIfNeeded( length );
        buffer.skipBytes( length );
    }

    @Override
    public int skipBytes( ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeByte( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeShort( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeMedium( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeInt( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeLong( long value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeChar( int value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeFloat( float value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeDouble( double value )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( ChannelBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( ChannelBuffer src, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( ChannelBuffer src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( byte[] src )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( byte[] src, int srcIndex, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeBytes( ByteBuffer src )
    {
        throw unsupportedOperation();
    }

    @Override
    public int writeBytes( InputStream in, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public int writeBytes( ScatteringByteChannel in, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public void writeZero( int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int length, ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        throw unsupportedOperation();
    }

    @Override
    public int bytesBefore( int index, int length, ChannelBufferIndexFinder indexFinder )
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer copy()
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer copy( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer slice()
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer slice( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ChannelBuffer duplicate()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuffer toByteBuffer( int index, int length )
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuffer[] toByteBuffers()
    {
        throw unsupportedOperation();
    }

    @Override
    public ByteBuffer[] toByteBuffers( int index, int length )
    {
        throw unsupportedOperation();
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
    public int compareTo( ChannelBuffer buffer )
    {
        return this.buffer.compareTo( buffer );
    }

    @Override
    public String toString()
    {
        return buffer.toString();
    }

    private InputStream asInputStream()
    {
        return new InputStream()
        {
            @Override
            public int read( byte[] b )
            {
                readBytes( b );
                return b.length;
            }

            @Override
            public int read( byte[] b, int off, int len )
            {
                readBytes( b, off, len );
                return len;
            }

            @Override
            public long skip( long n )
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
            public void close()
            {
            }

            @Override
            public synchronized void mark( int readlimit )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public synchronized void reset()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean markSupported()
            {
                return false;
            }

            @Override
            public int read()
            {
                return readByte();
            }
        };
    }
}
