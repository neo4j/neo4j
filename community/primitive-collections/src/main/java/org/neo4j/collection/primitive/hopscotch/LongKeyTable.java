/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

import static java.util.Arrays.fill;

public class LongKeyTable<VALUE>
        extends IntArrayBasedKeyTable<VALUE>
{
    public LongKeyTable( int capacity, VALUE singleValue )
    {
        super( capacity, 3, 32, singleValue );
    }

    @Override
    public long key( int index )
    {
        int actualIndex = index( index );
        long low = table[actualIndex]&0xFFFFFFFFL;
        long high = table[actualIndex+1]&0xFFFFFFFFL;
        return (high << 32) | low;
    }

    @Override
    protected void internalPut( int actualIndex, long key, VALUE value )
    {
        table[actualIndex] = (int)key;
        table[actualIndex+1] = (int)((key&0xFFFFFFFF00000000L) >>> 32);
    }

    @Override
    protected void internalRemove( int actualIndex )
    {
        fill( table, actualIndex, actualIndex+2 /*2 bytes, i.e. leave the hop bits as is*/, -1 );
    }

    @Override
    protected LongKeyTable<VALUE> newInstance( int newCapacity )
    {
        return new LongKeyTable<>( newCapacity, singleValue );
    }
}
