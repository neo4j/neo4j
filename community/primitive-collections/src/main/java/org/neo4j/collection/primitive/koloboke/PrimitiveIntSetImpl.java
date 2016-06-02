/*
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
package org.neo4j.collection.primitive.koloboke;

import com.koloboke.collect.IntCursor;
import com.koloboke.compile.ConcurrentModificationUnchecked;
import com.koloboke.compile.KolobokeSet;

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveIntVisitor;
import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

@SuppressWarnings( "ALL" )
@KolobokeSet
@ConcurrentModificationUnchecked
public abstract class PrimitiveIntSetImpl implements PrimitiveIntSet, IntPredicate
{
    public static PrimitiveIntSet withExpectedSize( int expectedSize )
    {
        return new KolobokePrimitiveIntSetImpl( expectedSize );
    }

    public abstract IntCursor cursor();
    public abstract boolean removeInt( int value );

    @Override
    public final boolean test( int value )
    {
        return contains( value );
    }

    @Override
    public final boolean addAll( PrimitiveIntIterator values )
    {
        boolean modified = false;
        while ( values.hasNext() )
        {
            modified |= add( values.next() );
        }
        return modified;
    }

    @Override
    public final void close()
    {
    }

    @Override
    public final <E extends Exception> void visitKeys( PrimitiveIntVisitor<E> visitor ) throws E
    {
        IntCursor cursor = cursor();
        while ( cursor.moveNext() && !visitor.visited( cursor.elem() ) );
    }

    @Override
    public final PrimitiveIntIterator iterator()
    {
        final IntCursor cursor = cursor();
        return new PrimitiveIntIterator()
        {
            boolean hasNext = cursor.moveNext();
            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public int next()
            {
                int elem = cursor.elem();
                hasNext = cursor.moveNext();
                return elem;
            }
        };
    }

    @Override
    public final boolean remove( int value )
    {
        return removeInt( value );
    }

    @Override
    public final boolean equals( Object obj )
    {
        if ( obj instanceof PrimitiveLongSet )
        {
            PrimitiveLongSet set = (PrimitiveLongSet) obj;
            if ( set.size() != this.size() )
            {
                return false;
            }
            IntCursor cursor = cursor();
            while ( cursor.moveNext() )
            {
                long value = cursor.elem();
                if ( !set.contains( value ) )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int hash = 1337;
        IntCursor cursor = cursor();
        while ( cursor.moveNext() )
        {
            hash += DEFAULT_HASHING.hash( cursor.elem() );
        }
        return hash;
    }
}
