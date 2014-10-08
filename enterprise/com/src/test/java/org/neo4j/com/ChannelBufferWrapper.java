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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;

/**
 * Wraps an {@link InMemoryLogChannel}, making it look like one {@link ByteBuf}.
 */
public class ChannelBufferWrapper extends ByteBuf
{
    private final InMemoryLogChannel delegate;

    public ChannelBufferWrapper( InMemoryLogChannel delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public int capacity()
    {
        return delegate.capacity();
    }

    @Override
    public ByteBuf capacity( int newCapacity )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxCapacity()
    {
        return delegate.capacity();
    }

    @Override
    public ByteBufAllocator alloc()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteOrder order()
    {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public ByteBuf order( ByteOrder endianness )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf unwrap()
    {
        throw new UnsupportedOperationException();
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
    public ByteBuf readerIndex( int readerIndex )
    {
        delegate.positionReader( readerIndex );
        return this;
    }

    @Override
    public int writerIndex()
    {
        return delegate.writerPosition();
    }

    @Override
    public ByteBuf writerIndex( int writerIndex )
    {
        delegate.positionWriter( writerIndex );
        return this;
    }

    @Override
    public ByteBuf setIndex( int readerIndex, int writerIndex )
    {
        delegate.positionReader( readerIndex );
        delegate.positionWriter( writerIndex );
        return this;
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
    public int maxWritableBytes()
    {
        return delegate.availableBytesToWrite();
    }

    @Override
    public boolean isReadable()
    {
        return delegate.writerPosition() > delegate.readerPosition();
    }

    @Override
    public boolean isReadable( int size )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWritable()
    {
        return delegate.writerPosition() < delegate.capacity();
    }

    @Override
    public boolean isWritable( int size )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf clear()
    {
        delegate.reset();
        return this;
    }

    @Override
    public ByteBuf markReaderIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf resetReaderIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf markWriterIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf resetWriterIndex()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf discardReadBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf discardSomeReadBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf ensureWritable( int minWritableBytes )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int ensureWritable( int minWritableBytes, boolean force )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean( int index )
    {
        throw new UnsupportedOperationException();
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
    public ByteBuf getBytes( int index, ByteBuf dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, length );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuf dst, int dstIndex, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, dstIndex, length );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, byte[] dst, int dstIndex, int length )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst, dstIndex, length );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, ByteBuffer dst )
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( dst );
            return this;
        }
        finally
        {
            delegate.positionReader( pos );
        }
    }

    @Override
    public ByteBuf getBytes( int index, OutputStream out, int length ) throws IOException
    {
        int pos = delegate.positionReader( index );
        try
        {
            readBytes( out, length );
            return this;
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
    public ByteBuf setBoolean( int index, boolean value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf setByte( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeByte( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setShort( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeShort( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setMedium( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeMedium( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setInt( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeInt( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setLong( int index, long value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeLong( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setChar( int index, int value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeChar( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setFloat( int index, float value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeFloat( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setDouble( int index, double value )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeDouble( value );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, length );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuf src, int srcIndex, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, srcIndex, length );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, byte[] src, int srcIndex, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src, srcIndex, length );
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public ByteBuf setBytes( int index, ByteBuffer src )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            writeBytes( src );
            return this;
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
    public ByteBuf setZero( int index, int length )
    {
        int pos = delegate.positionWriter( index );
        try
        {
            for ( int i = 0; i < length; i++ )
            {
                writeByte( 0 );
            }
            return this;
        }
        finally
        {
            delegate.positionWriter( pos );
        }
    }

    @Override
    public boolean readBoolean()
    {
        throw new UnsupportedOperationException();
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
    public ByteBuf readBytes( int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            return Unpooled.wrappedBuffer( array );
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ByteBuf readSlice( int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst )
    {
        readBytes( dst, dst.writableBytes() );
        return this;
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            dst.writeBytes( array );
            return this;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ByteBuf readBytes( ByteBuf dst, int dstIndex, int length )
    {
        dst.readerIndex( dstIndex );
        readBytes( dst, length );
        return this;
    }

    @Override
    public ByteBuf readBytes( byte[] dst )
    {
        try
        {
            delegate.get( dst, dst.length );
            return this;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ByteBuf readBytes( byte[] dst, int dstIndex, int length )
    {
        try
        {
            byte[] array = new byte[length];
            delegate.get( array, length );
            System.arraycopy( array, 0, dst, dstIndex, length );
            return this;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ByteBuf readBytes( ByteBuffer dst )
    {
        byte[] array = new byte[dst.remaining()];
        try
        {
            delegate.get( array, array.length );
            dst.put( array );
            return this;
        }
        catch ( ReadPastEndException e )
        {
            throw outOfBounds( e );
        }
    }

    @Override
    public ByteBuf readBytes( OutputStream out, int length ) throws IOException
    {
        byte[] array = new byte[length];
        try
        {
            delegate.get( array, length );
            out.write( array );
            return this;
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
    public ByteBuf skipBytes( int length )
    {
        delegate.positionReader( delegate.readerPosition()+length );
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
        try
        {
            delegate.put( (byte)value );
            return this;
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
    public ByteBuf writeShort( int value )
    {
        try
        {
            delegate.putShort( (short)value );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeMedium( int value )
    {
        try
        {
            delegate.putShort( (short)value );
            delegate.put( (byte)(value >>> 16) );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeInt( int value )
    {
        try
        {
            delegate.putInt( value );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeLong( long value )
    {
        try
        {
            delegate.putLong( value );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeChar( int value )
    {
        try
        {
            delegate.put( (byte)value );
            delegate.put( (byte)(value >>> 8) );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeFloat( float value )
    {
        try
        {
            delegate.putFloat( value );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeDouble( double value )
    {
        try
        {
            delegate.putDouble( value );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src )
    {
        writeBytes( src, src.readableBytes() );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int length )
    {
        try
        {
            byte[] array = new byte[length];
            src.readBytes( array );
            delegate.put( array, array.length );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeBytes( ByteBuf src, int srcIndex, int length )
    {
        src.readerIndex( srcIndex );
        writeBytes( src, length );
        return this;
    }

    @Override
    public ByteBuf writeBytes( byte[] src )
    {
        try
        {
            delegate.put( src, src.length );
            return this;
        }
        catch ( IOException e )
        {
            throw writeException( e );
        }
    }

    @Override
    public ByteBuf writeBytes( byte[] src, int srcIndex, int length )
    {
        if ( srcIndex > 0 )
        {
            byte[] array = new byte[length];
            System.arraycopy( src, 0, array, srcIndex, length );
            src = array;
        }
        writeBytes( src );
        return this;
    }

    @Override
    public ByteBuf writeBytes( ByteBuffer src )
    {
        byte[] array = new byte[src.remaining()];
        src.get( array );
        writeBytes( array );
        return this;
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
    public ByteBuf writeZero( int length )
    {
        for ( int i = 0; i < length; i++ )
        {
            writeByte( 0 );
        }
        return this;
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
    public int bytesBefore( byte value )
    {
        int index = indexOf( readerIndex(), writerIndex(), value );
        return index == -1 ? -1 : index - readerIndex();
    }

    @Override
    public int bytesBefore( int length, byte value )
    {
        return bytesBefore( readerIndex(), length, value );
    }

    @Override
    public int bytesBefore( int index, int length, byte value )
    {
        int foundIndex = indexOf( index, index+length, value );
        return foundIndex == -1 ? -1 : foundIndex - index;
    }

    @Override
    public int forEachByte( ByteBufProcessor processor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int forEachByte( int index, int length, ByteBufProcessor processor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int forEachByteDesc( ByteBufProcessor processor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int forEachByteDesc( int index, int length, ByteBufProcessor processor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf copy()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf copy( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf slice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf slice( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf duplicate()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nioBufferCount()
    {
        return 0;
    }

    @Override
    public ByteBuffer nioBuffer()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer nioBuffer( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer internalNioBuffer( int index, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] nioBuffers()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] nioBuffers( int index, int length )
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
    public boolean hasMemoryAddress()
    {
        return false;
    }

    @Override
    public long memoryAddress()
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
    public int compareTo( ByteBuf buffer )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf retain( int increment )
    {
        throw new UnsupportedOperationException( "Refcounting not supported by this buffer." );
    }

    @Override
    public boolean release()
    {
        throw new UnsupportedOperationException( "Refcounting not supported by this buffer." );
    }

    @Override
    public boolean release( int decrement )
    {
        throw new UnsupportedOperationException( "Refcounting not supported by this buffer." );
    }

    @Override
    public int refCnt()
    {
        throw new UnsupportedOperationException( "Refcounting not supported by this buffer." );
    }

    @Override
    public ByteBuf retain()
    {
        throw new UnsupportedOperationException( "Refcounting not supported by this buffer." );
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

        ChannelBufferWrapper that = (ChannelBufferWrapper) o;

        if ( delegate != null ? !delegate.equals( that.delegate ) : that.delegate != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return delegate != null ? delegate.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "ByteBufWrapper{" +
                "delegate=" + delegate +
                '}';
    }
}
