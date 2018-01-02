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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public class PrimitiveLongHashSet extends AbstractLongHopScotchCollection<Object> implements PrimitiveLongSet
{
    private final Object valueMarker;
    private final Monitor monitor;

    public PrimitiveLongHashSet( Table<Object> table, Object valueMarker, Monitor monitor )
    {
        super( table );
        this.valueMarker = valueMarker;
        this.monitor = monitor;
    }

    @Override
    public boolean add( long value )
    {
        return HopScotchHashingAlgorithm.put( table, monitor, DEFAULT_HASHING, value, valueMarker, this ) == null;
    }

    @Override
    public boolean addAll( PrimitiveLongIterator values )
    {
        boolean changed = false;
        while ( values.hasNext() )
        {
            changed |= HopScotchHashingAlgorithm.put( table, monitor, DEFAULT_HASHING, values.next(),
                    valueMarker, this ) == null;
        }
        return changed;
    }

    @Override
    public boolean contains( long value )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, value ) == valueMarker;
    }

    /**
     * Prefer using {@link #contains(long)} - this method is identical and required by the {@link org.neo4j.function.IntPredicate} interface
     *
     * @param value the input argument
     * @return true if the input argument matches the predicate, otherwise false
     */
    @Override
    public boolean test( long value )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, value ) == valueMarker;
    }

    @Override
    public boolean accept( long value )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, DEFAULT_HASHING, value ) == valueMarker;
    }

    @Override
    public boolean remove( long value )
    {
        return HopScotchHashingAlgorithm.remove( table, monitor, DEFAULT_HASHING, value ) == valueMarker;
    }

    @SuppressWarnings( "EqualsWhichDoesntCheckParameterClass" ) // yes it does
    @Override
    public boolean equals( Object other )
    {
        if ( typeAndSizeEqual( other ) )
        {
            PrimitiveLongHashSet that = (PrimitiveLongHashSet) other;
            LongKeyEquality equality = new LongKeyEquality( that );
            visitKeys( equality );
            return equality.isEqual();
        }
        return false;
    }

    private static class LongKeyEquality implements PrimitiveLongVisitor<RuntimeException>
    {
        private PrimitiveLongHashSet other;
        private boolean equal = true;

        public LongKeyEquality( PrimitiveLongHashSet that )
        {
            this.other = that;
        }

        @Override
        public boolean visited( long value )
        {
            equal = other.contains( value );
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
        HashCodeComputer hash = new HashCodeComputer();
        visitKeys( hash );
        return hash.hashCode();
    }

    private static class HashCodeComputer implements PrimitiveLongVisitor<RuntimeException>
    {
        private int hash = 1337;

        @Override
        public boolean visited( long value ) throws RuntimeException
        {
            hash += DEFAULT_HASHING.hash( value );
            return false;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }
    }
}
