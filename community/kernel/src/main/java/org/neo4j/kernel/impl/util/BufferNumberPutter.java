/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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