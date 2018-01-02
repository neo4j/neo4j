/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

public class VersionedLongKeyTable<VALUE> extends IntArrayBasedKeyTable<VALUE>
{
    private int version;

    public VersionedLongKeyTable( int h, VALUE singleValue )
    {
        super( 4, h, singleValue );
    }

    public VersionedLongKeyTable( int capacity, int h, VALUE singleValue, int version )
    {
        super( capacity, 4, h, singleValue );
        this.version = version;
    }

    @Override
    public long key( int index )
    {
        int actualIndex = index( index );
        return (long)table[actualIndex] | (table[actualIndex+1]) << 32;
    }

    @Override
    protected void internalPut( int actualIndex, long key, VALUE value )
    {
        table[actualIndex] = (int)key;
        table[actualIndex+1] = (int)((key&0xFFFFFFFF00000000L) >>> 32);
        table[actualIndex+2] = version;
    }

    @Override
    protected void internalRemove( int actualIndex )
    {
        fill( table, actualIndex, actualIndex+2, -1 );
    }

    @Override
    protected VersionedLongKeyTable<VALUE> newInstance( int newCapacity )
    {
        return new VersionedLongKeyTable<>( newCapacity, h, singleValue, version );
    }

    @Override
    public int version()
    {
        return this.version;
    }

    @Override
    public int version( int index )
    {
        return table[index( index )+2];
    }
}
