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

import com.koloboke.collect.LongIterator;
import com.koloboke.compile.ConcurrentModificationUnchecked;
import com.koloboke.compile.KolobokeSet;
import com.koloboke.compile.MethodForm;
import com.koloboke.compile.NullKeyAllowed;
import com.koloboke.compile.hash.algo.openaddressing.DoubleHashing;
import com.koloboke.compile.hash.algo.openaddressing.QuadraticHashing;

import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

@KolobokeSet
@ConcurrentModificationUnchecked
@DoubleHashing
public abstract class PrimitiveLongSetImpl implements PrimitiveLongSet, LongPredicate
{
    public static PrimitiveLongSet withExpectedSize(int expectedSize)
    {
        return new KolobokePrimitiveLongSetImpl( expectedSize );
    }

    public abstract void forEach( LongConsumer consumer );
    public abstract boolean contains( long value );
    public abstract boolean add( long value );
    @MethodForm( "iterator" )
    public abstract LongIterator longIterator();
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
        try
        {
            forEach( value -> {
                try
                {
                    visitor.visited( value );
                }
                catch ( Exception e )
                {
                    throw new InnerException( e );
                }
            } );
        }
        catch ( InnerException e )
        {
            throw (E) e.getCause();
        }
    }

    @Override
    public final PrimitiveLongIterator iterator()
    {
        final LongIterator itr = longIterator();
        return new PrimitiveLongIterator()
        {
            @Override
            public boolean hasNext()
            {
                return itr.hasNext();
            }

            @Override
            public long next()
            {
                return itr.nextLong();
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
            LongIterator itr = longIterator();
            while ( itr.hasNext() )
            {
                long value = itr.nextLong();
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
        LongIterator itr = longIterator();
        while ( itr.hasNext() )
        {
            hash += DEFAULT_HASHING.hash( itr.nextLong() );
        }
        return hash;
    }
}
