/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.util.primitive.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.util.primitive.collection.base.PrimitiveIntIteratorForArray;
import org.neo4j.util.primitive.collection.base.PrimitiveLongIteratorForArray;
import org.neo4j.util.primitive.collection.hopscotch.PrimitiveLongHashSet;
import org.neo4j.util.primitive.collection.hopscotch.PrimitiveLongIntHashMap;
import org.neo4j.util.primitive.collection.hopscotch.PrimitiveLongIntMap;
import org.neo4j.util.primitive.function.FunctionFromPrimitiveInt;
import org.neo4j.util.primitive.function.FunctionFromPrimitiveLong;
import org.neo4j.util.primitive.function.PrimitiveLongPredicate;

/**
 * Convenient factory for common primitive sets and maps.
 */
public class Primitive
{
    // Some example would be...
    public static PrimitiveLongSet longSet()
    {
        return new PrimitiveLongHashSet();
    }

    public static PrimitiveLongIntMap longIntMap()
    {
        return new PrimitiveLongIntHashMap();
    }

    public static PrimitiveIntIterator intIteratorOver( int... values )
    {
        return new PrimitiveIntIteratorForArray( values );
    }

    public static PrimitiveLongIterator longIteratorOver( long... values )
    {
        return new PrimitiveLongIteratorForArray( values );
    }

    public static <T> Iterator<T> map( final FunctionFromPrimitiveLong<T> mapFunction,
            final PrimitiveLongIterator source )
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return source.hasNext();
            }

            @Override
            public T next()
            {
                return mapFunction.apply( source.next() );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static <T> Iterator<T> map( final FunctionFromPrimitiveInt<T> mapFunction,
            final PrimitiveIntIterator source )
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return source.hasNext();
            }

            @Override
            public T next()
            {
                return mapFunction.apply( source.next() );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the given iterator's single element or {@code itemIfNone} if no
     * element found. If there is more than one element in the iterator a
     * {@link NoSuchElementException} will be thrown.
     *
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in {@code iterator}, or {@code itemIfNone} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static long single( PrimitiveLongIterator iterator, long itemIfNone )
    {
        if ( iterator.hasNext() )
        {
            long result = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one element in " +
                        iterator + ". First element is '" + result +
                        "' and the second element is '" + iterator.next() + "'" );
            }
            return result;
        }
        return itemIfNone;
    }

    /**
     * Returns a new iterator with all elements found in the input iterator that are accepted by the given predicate
     *
     * @param predicate predicate to use for selecting elements
     * @param iterator input source of elements to be filtered
     * @return new iterator that contains exactly all elements from iterator that are accepted by predicate
     */
    public static PrimitiveLongIterator filter( final PrimitiveLongPredicate predicate,
                                                final PrimitiveLongIterator iterator )
    {
        return new PrimitiveLongIterator()
        {
            long next = -1;
            boolean hasNext = false;

            {
                computeNext();
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public long next()
            {
                if ( hasNext )
                {
                    long result = next;
                    computeNext();
                    return result;
                }
                else
                {
                    throw new NoSuchElementException();
                }
            }

            private void computeNext()
            {
                while ( iterator.hasNext() )
                {
                    next = iterator.next();
                    if ( predicate.accept( next ) )
                    {
                        hasNext = true;
                        return;
                    }
                }
                hasNext = false;
            }
        };
    }
}
