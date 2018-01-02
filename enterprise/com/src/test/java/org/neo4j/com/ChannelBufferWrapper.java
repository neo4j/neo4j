/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;

import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;

/**
 * Wraps an {@link InMemoryLogChannel}, making it look like one {@link ChannelBuffer}.
 */
public class ChannelBufferWrapper implements ChannelBuffer
{
    private final InMemoryLogChannel delegate;

    public ChannelBufferWrapper( InMemoryLogChannel delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public ChannelBufferFactory factory()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int capacity()
    {
        return delegate.capacity();
    }

    @Override
    public ByteOrder order()
    {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public boolean isDirect()
    {
        return false;
    }

    @Override
    public int readerIndex()
    {
        return delegate.readerPosition();
    }

    @Override
    public void readerIndex( int readerIndex )
    {
        delegate.positionReader( readerIndex );
    }

    @Override
    public int writerIndex()
    {
        return delegate.writerPosition();
    }

    @Override
    public void writerIndex( int writerIndex )
    {
        delegate.positionWriter( writerIndex );
    }

    @Override
    public void setIndex( int readerIndex, int writerIndex )
    {
        delegate.positionReader( readerIndex );
        delegate.positionWriter( writerIndex );
    }

    @Override
    public int readableBytes()
    {
        return delegate.availableBytesToRead();
    }

    @Override
    public int writableBytes()
    {
        return delegate.availableBytesToWrite();
    }

    @Override
    public boolean readable()
    {
        return delegate.writerPosition() > delegate.readerPosition();
    }

    @Override
    public boolean writable()
    {
        return delegate.writerPosition() < delegate.capacity();
    }

    @Override
    public void clear()
    {
        delegate.reset();
    }

    @Override
    public void markReaderIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetReaderIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markWriterIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetWriterIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void discardReadBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureWritableBytes( int writableBytes )
    {
        boolean availableBytes = delegate.availableBytesToWrite() < writableBytes;
        if ( availableBytes )
        {
            throw new IndexOutOfBoundsException( "Wanted " + writableBytes + " to be available for writing, " +
                    "but there were only " + availableBytes + " available" );
        }
    }

    @Override
    public byte getByte( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readByte();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public short getUnsignedByte( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readUnsignedByte();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public short getShort( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readShort();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int getUnsignedShort( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readUnsignedShort();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int getMedium( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readMedium();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int getUnsignedMedium( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readUnsignedMedium();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int getInt( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readInt();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public long getUnsignedInt( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readUnsignedInt();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public long getLong( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readLong();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public char getChar( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readChar();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public float getFloat( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readFloat();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public double getDouble( int index )
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readDouble();
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, length );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, ChannelBuffer dst, int dstIndex, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, dstIndex, length );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, byte[] dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, dstIndex, length );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, ByteBuffer dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void getBytes( int index, OutputStream out, int length ) throws IOException
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( out, length );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int getBytes( int index, GatheringByteChannel out, int length ) throws IOException
    {
        int pos = delegate.positionReader( index );
        try
        {
            return readBytes( out, length );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public void setByte( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeByte( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setShort( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeShort( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setMedium( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeMedium( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setInt( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeInt( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setLong( int index, long value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeLong( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setChar( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeChar( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setFloat( int index, float value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeFloat( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setDouble( int index, double value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeDouble( value );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, ChannelBuffer src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, length );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, ChannelBuffer src, int srcIndex, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, srcIndex, length );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, byte[] src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, byte[] src, int srcIndex, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, srcIndex, length );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setBytes( int index, ByteBuffer src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public int setBytes( int index, InputStream in, int length ) throws IOException
    {
        int pos = delegate.positionWriter( index );
        try
        {
            return writeBytes( in, length );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public int setBytes( int index, ScatteringByteChannel in, int length ) throws IOException
    {
        int pos = delegate.positionWriter( index );
        try
        {
            return writeBytes( in, length );
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public void setZero( int index, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            for ( int i = 0; i < length; i++ )
            {
                writeByte( 0 );
            }
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public byte readByte()
    {
        try
        {
            return delegate.get();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    private IndexOutOfBoundsException outOfBounds( ReadPastEndException e )
    {
        return new IndexOutOfBoundsException( "Tried to read past the end " + e );
    }

    @Override
    public short readUnsignedByte()
    {
        try
        {
            return (short) (delegate.get()&0xFF);
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public short readShort()
    {
        try
        {
            return delegate.getShort();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public int readUnsignedShort()
    {
        try
        {
            return delegate.getShort()&0xFFFF;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public int readMedium()
    {
        try
        {
            int low = delegate.getShort();
            int high = delegate.get();
            return low | (high << 16);
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public int readUnsignedMedium()
    {
        return readMedium();
    }

    @Override
    public int readInt()
    {
        try
        {
            return delegate.getInt();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public long readUnsignedInt()
    {
        try
        {
            return delegate.getInt()&0xFFFFFFFFL;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public long readLong()
    {
        try
        {
            return delegate.getLong();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public char readChar()
    {
        try
        {
            short low = delegate.get();
            short high = delegate.get();
            return (char)(low | (high << 8));
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public float readFloat()
    {
        try
        {
            return delegate.getFloat();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public double readDouble()
    {
        try
        {
            return delegate.getDouble();
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ChannelBuffer readBytes( int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            return new ByteBufferBackedChannelBuffer( ByteBuffer.wrap( array ) );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ChannelBuffer readBytes( ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer readSlice( int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer readSlice( ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBytes( ChannelBuffer dst )
    {
        readBytes( dst, dst.writableBytes() );
    }

    @Override
    public void readBytes( ChannelBuffer dst, int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            dst.writeBytes( array );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public void readBytes( ChannelBuffer dst, int dstIndex, int length )
    {
        dst.readerIndex( dstIndex );
        readBytes( dst, length );
    }

    @Override
    public void readBytes( byte[] dst )
    {
        try
        {
            delegate.get( dst, dst.length );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public void readBytes( byte[] dst, int dstIndex, int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            System.arraycopy( array, 0, dst, dstIndex, length );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public void readBytes( ByteBuffer dst )
    {
        byte[] array = new byte[dst.remaining()];
        try
        {
            delegate.get( array, array.length );
            dst.put( array );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public void readBytes( OutputStream out, int length ) throws IOException
    {
        byte[] array = new byte[length];
        try
        {
            delegate.get( array, length );
            out.write( array );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public int readBytes( GatheringByteChannel out, int length ) throws IOException
    {
        byte[] array = new byte[length];
        try
        {
            delegate.get( array, length );
            return out.write( ByteBuffer.wrap( array ) );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public void skipBytes( int length )
    {
        delegate.positionReader( delegate.readerPosition()+length );
    }

    @Override
    public int skipBytes( ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte( int value )
    {
        try
        {
            delegate.put( (byte)value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    private RuntimeException writeException( IOException e )
    {
        return new RuntimeException( e );
    }

    @Override
    public void writeShort( int value )
    {
        try
        {
            delegate.putShort( (short)value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeMedium( int value )
    {
        try
        {
            delegate.putShort( (short)value );
            delegate.put( (byte)(value >>> 16) );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeInt( int value )
    {
        try
        {
            delegate.putInt( value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeLong( long value )
    {
        try
        {
            delegate.putLong( value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeChar( int value )
    {
        try
        {
            delegate.put( (byte)value );
            delegate.put( (byte)(value >>> 8) );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeFloat( float value )
    {
        try
        {
            delegate.putFloat( value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeDouble( double value )
    {
        try
        {
            delegate.putDouble( value );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeBytes( ChannelBuffer src )
    {
        writeBytes( src, src.readableBytes() );
    }

    @Override
    public void writeBytes( ChannelBuffer src, int length )
    {
        try
        {
            byte[] array = new byte[length];
            src.readBytes( array );
            delegate.put( array, array.length );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeBytes( ChannelBuffer src, int srcIndex, int length )
    {
        src.readerIndex( srcIndex );
        writeBytes( src, length );
    }

    @Override
    public void writeBytes( byte[] src )
    {
        try
        {
            delegate.put( src, src.length );
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public void writeBytes( byte[] src, int srcIndex, int length )
    {
        if ( srcIndex > 0 )
        {
            byte[] array = new byte[length];
            System.arraycopy( src, 0, array, srcIndex, length );
            src = array;
        }
        writeBytes( src );
    }

    @Override
    public void writeBytes( ByteBuffer src )
    {
        byte[] array = new byte[src.remaining()];
        src.get( array );
        writeBytes( array );
    }

    @Override
    public int writeBytes( InputStream in, int length ) throws IOException
    {
        byte[] array = new byte[length];
        int read = in.read( array );
        writeBytes( array, 0, read );
        return read;
    }

    @Override
    public int writeBytes( ScatteringByteChannel in, int length ) throws IOException
    {
        byte[] array = new byte[length];
        int read = in.read( ByteBuffer.wrap( array ) );
        writeBytes( array, 0, read );
        return read;
    }

    @Override
    public void writeZero( int length )
    {
        for ( int i = 0; i < length; i++ )
        {
            writeByte( 0 );
        }
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, byte value )
    {
        int pos = delegate.positionReader( fromIndex );
        try
        {
            while ( delegate.readerPosition() < toIndex )
            {
                int thisPos = delegate.readerPosition();
                if ( delegate.get() == value )
                {
                    return thisPos;
                }
            }
            return -1;
        }
        catch ( ReadPastEndException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public int indexOf( int fromIndex, int toIndex, ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bytesBefore( byte value )
    {
        int index = indexOf( readerIndex(), writerIndex(), value );
        return index == -1 ? -1 : index - readerIndex();
    }

    @Override
    public int bytesBefore( ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        return bytesBefore( readerIndex(), length, value );
    }

    @Override
    public int bytesBefore( int length, ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        int foundIndex = indexOf( index, index+length, value );
        return foundIndex == -1 ? -1 : foundIndex - index;
    }

    @Override
    public int bytesBefore( int index, int length, ChannelBufferIndexFinder indexFinder )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer copy()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer copy( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer slice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer slice( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelBuffer duplicate()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer toByteBuffer( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] toByteBuffers()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] toByteBuffers( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasArray()
    {
        return false;
    }

    @Override
    public byte[] array()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int arrayOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString( Charset charset )
    {
        return toString();
    }

    @Override
    public String toString( int index, int length, Charset charset )
    {
        return toString();
    }

    @Override
    public String toString( String charsetName )
    {
        return toString();
    }

    @Override
    public String toString( String charsetName, ChannelBufferIndexFinder terminatorFinder )
    {
        return toString();
    }

    @Override
    public String toString( int index, int length, String charsetName )
    {
        return toString();
    }

    @Override
    public String toString( int index, int length, String charsetName, ChannelBufferIndexFinder terminatorFinder )
    {
        return toString();
    }

    @Override
    public int compareTo( ChannelBuffer buffer )
    {
        throw new UnsupportedOperationException();
    }
}
