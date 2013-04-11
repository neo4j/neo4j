/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Alexander Yastrebov
 */
final class CompoundCodec {

    private CompoundCodec()
    {
    }

    static final class Decoder
    {
        private ByteBuffer buffer;

        Decoder( byte[] bytes )
        {
            buffer = ByteBuffer.wrap( bytes );
        }

        public boolean hasNext()
        {
            return buffer.remaining() > 0;
        }

        public Property next()
        {
            int key = 0;
            key |= ( buffer.get() & 0xFF ) << 16;
            key |= ( buffer.get() & 0xFF ) << 8;
            key |= ( buffer.get() & 0xFF ) << 0;

            long first = buffer.getLong();
            int cnt = PropertyType.getPropertyType( first, false ).calculateNumberOfBlocksUsed( first );

            long[] valueBlocks = new long[cnt];
            valueBlocks[0] = first;
            for ( int i = 1 ; i < cnt ; i++ )
            {
                valueBlocks[i] = buffer.getLong();
            }
            int valueRecordCount = buffer.getInt();

            return new Property( key, valueBlocks, valueRecordCount );
        }
    }

    static final class Coder
    {
        private List<Property> properties;

        Coder( int propertyCount )
        {
            properties = new ArrayList<Property>( propertyCount );
        }

        public void add( int key, long[] valueBlocks, int valueRecordCount )
        {
            properties.add( new Property( key, valueBlocks, valueRecordCount ) );
        }

        public byte[] getBytes()
        {
            int total = 0;
            for ( Property p : properties )
            {
                total += 3 + p.valueBlocks.length * 8 + 4;
            }

            ByteBuffer buffer = ByteBuffer.allocate( total );
            for ( Property p : properties )
            {
                buffer.put( (byte)( (p.key >>> 16) & 0xFF ) );
                buffer.put( (byte)( (p.key >>> 8 ) & 0xFF ) );
                buffer.put( (byte)( (p.key >>> 0 ) & 0xFF ) );

                for ( long v : p.valueBlocks )
                {
                    buffer.putLong( v );
                }
                buffer.putInt( p.valueRecordCount );
            }
            return buffer.array();
        }
    }

    static class Property
    {
        private int key;
        private long[] valueBlocks;
        private int valueRecordCount;

        private Property( int key, long[] valueBlocks, int valueRecordCount )
        {
            this.key = key;
            this.valueBlocks = valueBlocks;
            this.valueRecordCount = valueRecordCount;
        }

        public int getKey()
        {
            return key;
        }

        public long[] getValueBlocks()
        {
            return valueBlocks;
        }

        public int getValueRecordCount()
        {
            return valueRecordCount;
        }
    }
}
