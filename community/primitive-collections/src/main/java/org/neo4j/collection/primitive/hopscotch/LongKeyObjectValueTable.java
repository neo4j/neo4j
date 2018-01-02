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

public class LongKeyObjectValueTable<VALUE> extends LongKeyTable<VALUE>
{
    private VALUE[] values;

    public LongKeyObjectValueTable( int capacity )
    {
        super( capacity, null );
    }

    @Override
    public VALUE value( int index )
    {
        return values[index];
    }

    @Override
    public void put( int index, long key, VALUE value )
    {
        super.put( index, key, value );
        values[index] = value;
    }

    @Override
    public VALUE putValue( int index, VALUE value )
    {
        VALUE previous = values[index];
        values[index] = value;
        return previous;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        values[toIndex] = values[fromIndex];
        values[fromIndex] = null;
        return super.move( fromIndex, toIndex );
    }

    @Override
    public VALUE remove( int index )
    {
        super.remove( index );
        VALUE existing = values[index];
        values[index] = null;
        return existing;
    }

    @Override
    protected LongKeyObjectValueTable<VALUE> newInstance( int newCapacity )
    {
        return new LongKeyObjectValueTable<>( newCapacity );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void initializeTable()
    {
        super.initializeTable();
        values = (VALUE[]) new Object[capacity];
    }

    @Override
    protected void clearTable()
    {
        super.clearTable();
        fill( values, null );
    }
}
