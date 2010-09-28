package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;

public enum BufferNumberPutter
{
    BYTE( Byte.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.put( value.byteValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value ) throws IOException
        {
            buffer.put( value.byteValue() );
        }
    },
    SHORT( Short.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.putShort( value.shortValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value )
        {
            throw new UnsupportedOperationException();
        }
    },
    INT( Integer.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.putInt( value.intValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value ) throws IOException
        {
            buffer.putInt( value.intValue() );
        }
    },
    LONG( Long.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.putLong( value.longValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value ) throws IOException
        {
            buffer.putLong( value.longValue() );
        }
    },
    FLOAT( Float.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.putFloat( value.floatValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value ) throws IOException
        {
            buffer.putFloat( value.floatValue() );
        }
    },
    DOUBLE( Double.SIZE )
    {
        @Override
        public void put( ByteBuffer buffer, Number value )
        {
            buffer.putDouble( value.doubleValue() );
        }

        @Override
        public void put( LogBuffer buffer, Number value ) throws IOException
        {
            buffer.putDouble( value.doubleValue() );
        }
    };

    private final int size;

    private BufferNumberPutter( int size )
    {
        this.size = size/8;
    }
    
    public int size()
    {
        return this.size;
    }
    
    public abstract void put( ByteBuffer buffer, Number value );

    public abstract void put( LogBuffer buffer, Number value ) throws IOException;
}