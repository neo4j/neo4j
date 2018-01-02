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

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public class PrimitiveIntObjectHashMap<VALUE> extends AbstractIntHopScotchCollection<VALUE>
        implements PrimitiveIntObjectMap<VALUE>
{
    private final Monitor monitor;

    public PrimitiveIntObjectHashMap( Table<VALUE> table, Monitor monitor )
    {
        super( table );
        this.monitor = monitor;
    }

    @Override
    public VALUE put( int key, VALUE value )
    {
        return HopScotchHashingAlgorithm.put( table, monitor, DEFAULT_HASHING, key, value, this );
    }

    @Override
    public boolean containsKey( int key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key ) != null;
    }

    @Override
    public VALUE get( int key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, key );
    }

    @Override
    public VALUE remove( int key )
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
    public <E extends Exception> void visitEntries( PrimitiveIntObjectVisitor<VALUE, E> visitor ) throws E
    {
        long nullKey = table.nullKey();
        int capacity = table.capacity();
        for ( int i = 0; i < capacity; i++ )
        {
            int key = (int) table.key( i );
            if ( key != nullKey && visitor.visited( key, table.value( i ) ) )
            {
                return;
            }
        }
    }

    @SuppressWarnings( "EqualsWhichDoesntCheckParameterClass" ) // yes it does
    @Override
    public boolean equals( Object other )
    {
        if ( typeAndSizeEqual( other ) )
        {
            PrimitiveIntObjectHashMap<?> that = (PrimitiveIntObjectHashMap<?>) other;
            IntObjEquality<VALUE> equality = new IntObjEquality<VALUE>( that );
            visitEntries( equality );
            return equality.isEqual();
        }
        return false;
    }

    private static class IntObjEquality<T> implements PrimitiveIntObjectVisitor<T,RuntimeException>
    {
        private PrimitiveIntObjectHashMap other;
        private boolean equal = true;

        public IntObjEquality( PrimitiveIntObjectHashMap that )
        {
            this.other = that;
        }

        @Override
        public boolean visited( int key, T value )
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
        HashCodeComputer<VALUE> hash = new HashCodeComputer<VALUE>();
        visitEntries( hash );
        return hash.hashCode();
    }

    private static class HashCodeComputer<T> implements PrimitiveIntObjectVisitor<T,RuntimeException>
    {
        private int hash = 1337;

        @Override
        public boolean visited( int key, T value ) throws RuntimeException
        {
            hash += DEFAULT_HASHING.hash( key + value.hashCode() );
            return false;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }
    }
}
