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

import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public class PrimitiveLongIntHashMap extends AbstractLongHopScotchCollection<int[]> implements PrimitiveLongIntMap
{
    private final int[] transport = new int[1];
    private final Monitor monitor;

    public PrimitiveLongIntHashMap( Table<int[]> table, Monitor monitor )
    {
        super( table );
        this.monitor = monitor;
    }

    @Override
    public int put( long key, int value )
    {
        return unpack( HopScotchHashingAlgorithm.put( table, monitor, DEFAULT_HASHING, key, pack( value ), this ) );
    }

    @Override
    public boolean containsKey( long key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key ) != null;
    }

    @Override
    public int get( long key )
    {
        return unpack( HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key ) );
    }

    @Override
    public int remove( long key )
    {
        return unpack( HopScotchHashingAlgorithm.remove( table, monitor, DEFAULT_HASHING, key ) );
    }

    @Override
    public int size()
    {
        return table.size();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }

    private int[] pack( int value )
    {
        transport[0] = value;
        return transport;
    }

    private int unpack( int[] result )
    {
        return result != null ? result[0] : LongKeyIntValueTable.NULL;
    }

    @Override
    public void visitEntries( PrimitiveLongIntVisitor visitor )
    {
        long nullKey = table.nullKey();
        int capacity = table.capacity();
        for ( int i = 0; i < capacity; i++ )
        {
            int[] value = table.value( i );
            if ( value != null )
            {
                long key = table.key( i );
                if ( key != nullKey )
                {
                    visitor.visited( key, value[0] );
                }
            }
        }
    }
}
