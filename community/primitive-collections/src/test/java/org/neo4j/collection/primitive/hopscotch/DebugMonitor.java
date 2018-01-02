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

import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static java.lang.String.format;

class DebugMonitor extends Monitor.Adapter
{
    // This is not the place to use primitive collections, since we're debugging issues in them
    private final Set<Integer> indexes = new HashSet<>();
    private final Set<Long> values = new HashSet<>();

    public DebugMonitor( int[] relevantIndexes, long[] relevantValues )
    {
        for ( int index : relevantIndexes )
        {
            indexes.add( index );
        }
        for ( long value : relevantValues )
        {
            values.add( value );
        }
    }

    private String hopBitsAsString( long hopBits )
    {
        StringBuilder builder = new StringBuilder( "[" );
        while ( hopBits > 0 )
        {
            int indexDistance = Long.numberOfTrailingZeros( hopBits );
            hopBits &= hopBits-1;
            builder.append( builder.length() > 1 ? "," : "" ).append( indexDistance+1 );
        }
        return builder.append( "]" ).toString();
    }

    private String hopBitsAsString( long oldHopBits, long newHopBits )
    {
        return hopBitsAsString( oldHopBits ) + " > " + hopBitsAsString( newHopBits );
    }

    @Override
    public boolean placedAtFreedIndex( int intendedIndex, long newHopBits, long key, int actualIndex )
    {
        if ( indexes.contains( intendedIndex ) || indexes.contains( actualIndex ) || values.contains( key ) )
        {
            System.out.println( format( "[%d] --> |%d| <-- %d  %s", intendedIndex, actualIndex, key,
                    hopBitsAsString( newHopBits ) ) );
        }
        return true;
    }

    @Override
    public boolean placedAtFreeAndIntendedIndex( long key, int index )
    {
        if ( indexes.contains( index ) || values.contains( key ) )
        {
            System.out.println( format( "[%d] <-- %d", index, key ) );
        }
        return true;
    }

    @Override
    public boolean pushedToFreeIndex( int intendedIndex, long oldNeighborHopBits, long newNeighborHopBits,
            int neighborIndex, long key, int fromIndex, int toIndex )
    {
        if ( indexes.contains( intendedIndex ) || indexes.contains( neighborIndex ) ||
                indexes.contains( fromIndex ) || indexes.contains( toIndex ) || values.contains( key ) )
        {
            System.out.println( format( "[%d] --- [%d] -->> [%d] --> %d --> [%d]  %s",
                    intendedIndex, neighborIndex, fromIndex, key, toIndex,
                    hopBitsAsString( oldNeighborHopBits, newNeighborHopBits ) ) );
        }
        return true;
    }

    @Override
    public boolean pulledToFreeIndex( int intendedIndex, long newHopBits, long key, int fromIndex, int toIndex )
    {
        if ( indexes.contains( intendedIndex ) ||
                indexes.contains( fromIndex ) || indexes.contains( toIndex ) || values.contains( key ) )
        {
            System.out.println( format( "[%d] --- [%d] <<-- %d <-- [%d]  %s",
                    intendedIndex, toIndex, key, fromIndex,
                    hopBitsAsString( newHopBits ) ) );
        }
        return true;
    }
}
