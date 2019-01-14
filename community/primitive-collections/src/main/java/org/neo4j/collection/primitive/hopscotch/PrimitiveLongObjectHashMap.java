/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public class PrimitiveLongObjectHashMap<VALUE> extends AbstractLongHopScotchCollection<VALUE>
        implements PrimitiveLongObjectMap<VALUE>
{
    private final Monitor monitor;

    public PrimitiveLongObjectHashMap( Table<VALUE> table, Monitor monitor )
    {
        super( table );
        this.monitor = monitor;
    }

    @Override
    public VALUE put( long key, VALUE value )
    {
        return HopScotchHashingAlgorithm.put( table, monitor, DEFAULT_HASHING, key, value, this );
    }

    @Override
    public boolean containsKey( long key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key ) != null;
    }

    @Override
    public VALUE get( long key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key );
    }

    @Override
    public VALUE remove( long key )
    {
        return HopScotchHashingAlgorithm.remove( table, monitor, DEFAULT_HASHING, key );
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

    @Override
    public <E extends Exception> void visitEntries( PrimitiveLongObjectVisitor<VALUE, E> visitor ) throws E
    {
        long nullKey = table.nullKey();
        int capacity = table.capacity();
        for ( int i = 0; i < capacity; i++ )
        {
            long key = table.key( i );
            if ( key != nullKey && visitor.visited( key, table.value( i ) ) )
            {
                return;
            }
        }
    }

    @Override
    public Iterable<VALUE> values()
    {
        return new ValueIterable( this );
    }

    @SuppressWarnings( "EqualsWhichDoesntCheckParameterClass" ) // yes it does
    @Override
    public boolean equals( Object other )
    {
        if ( typeAndSizeEqual( other ) )
        {
            PrimitiveLongObjectHashMap<?> that = (PrimitiveLongObjectHashMap<?>) other;
            LongObjEquality<VALUE> equality = new LongObjEquality<>( that );
            visitEntries( equality );
            return equality.isEqual();
        }
        return false;
    }

    private static class LongObjEquality<T> implements PrimitiveLongObjectVisitor<T,RuntimeException>
    {
        private PrimitiveLongObjectHashMap other;
        private boolean equal = true;

        LongObjEquality( PrimitiveLongObjectHashMap that )
        {
            this.other = that;
        }

        @Override
        public boolean visited( long key, T value )
        {
            Object otherValue = other.get( key );
            equal = otherValue == value || (otherValue != null && otherValue.equals( value ) );
            return !equal;
        }

        public boolean isEqual()
        {
            return equal;
        }
    }

    @Override
    public int hashCode()
    {
        HashCodeComputer<VALUE> hash = new HashCodeComputer<>();
        visitEntries( hash );
        return hash.hashCode();
    }

    private static class HashCodeComputer<T> implements PrimitiveLongObjectVisitor<T,RuntimeException>
    {
        private int hash = 1337;

        @Override
        public boolean visited( long key, T value ) throws RuntimeException
        {
            hash += DEFAULT_HASHING.hashSingleValueToInt( key + value.hashCode() );
            return false;
        }

        @Override
        public int hashCode()
        {
            return hash;
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
            HashCodeComputer<?> that = (HashCodeComputer<?>) o;
            return hash == that.hash;
        }
    }

    private class ValueIterable implements Iterable<VALUE>
    {
        private final PrimitiveLongObjectHashMap<VALUE> map;

        ValueIterable( PrimitiveLongObjectHashMap<VALUE> map )
        {
            this.map = map;
        }

        @Override
        public Iterator<VALUE> iterator()
        {
            return new Iterator<VALUE>()
            {
                private final PrimitiveLongIterator iterator = map.iterator();

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public VALUE next()
                {
                    return map.get( iterator.next() );
                }
            };
        }
    }
}
