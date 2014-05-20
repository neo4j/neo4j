package org.neo4j.io.pagecache.impl.common;

import java.nio.ByteBuffer;

/** A page backed by a simple byte buffer. */
public class ByteBufferPage implements Page
{
    private final ByteBuffer buffer;

    public ByteBufferPage( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    @Override
    public byte getByte( int offset )
    {
        return buffer.get(offset);
    }

    @Override
    public long getLong( int offset )
    {
        return buffer.getLong(offset);
    }

    @Override
    public void putLong( long value, int offset )
    {
        buffer.putLong( offset, value );
    }

    @Override
    public int getInt( int offset )
    {
        return buffer.getInt(offset);
    }

    @Override
    public void putInt( int value, int offset )
    {
        buffer.putInt( offset, value );
    }

    @Override
    public void getBytes( byte[] data, int offset )
    {
        buffer.get( data, offset, data.length );
    }

    @Override
    public void putBytes( byte[] data, int offset )
    {
        buffer.put( data, offset, data.length );
    }

    @Override
    public void putByte( byte value, int offset )
    {
        buffer.put(offset, value);
    }
}
