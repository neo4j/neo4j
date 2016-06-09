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

import com.koloboke.collect.LongCursor;
import com.koloboke.compile.ConcurrentModificationUnchecked;
import com.koloboke.compile.KolobokeSet;

import java.util.function.LongPredicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.collection.primitive.base.Hashing;

@SuppressWarnings( "ALL" )
@KolobokeSet
@ConcurrentModificationUnchecked
public abstract class PrimitiveLongSetImpl implements PrimitiveLongSet, LongPredicate
{
    public static PrimitiveLongSet withExpectedSize( int expectedSize )
    {
        return new KolobokePrimitiveLongSetImpl( expectedSize );
    }

    public abstract LongCursor cursor();
    public abstract boolean removeLong( long value );

    @Override
    public final boolean test( long value )
    {
        return contains( value );
    }

    @Override
    public final boolean addAll( PrimitiveLongIterator values )
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
    public final <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
    {
        LongCursor cursor = cursor();
        while ( cursor.moveNext() && !visitor.visited( cursor.elem() ) );
    }

    @Override
    public final PrimitiveLongIterator iterator()
    {
        final LongCursor cursor = cursor();
        return new PrimitiveLongIterator()
        {
            boolean hasNext = cursor.moveNext();
            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public long next()
            {
                long elem = cursor.elem();
                hasNext = cursor.moveNext();
                return elem;
            }
        };
    }

    @Override
    public final boolean remove( long value )
    {
        return removeLong( value );
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
            LongCursor cursor = cursor();
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
        LongCursor cursor = cursor();
        while ( cursor.moveNext() )
        {
            hash += Hashing.xorShift( cursor.elem() );
        }
        return hash;
    }
}
