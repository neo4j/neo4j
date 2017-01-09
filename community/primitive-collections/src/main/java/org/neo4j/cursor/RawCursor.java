/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cursor;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntSet;

/**
 * A cursor is an object that moves to point to different locations in a data structure.
 * The abstraction originally comes from mechanical slide rules, which have a "cursor" which
 * slides to point to different positions on the ruler.
 * <p>
 * Each position a cursor points to is referred to as a "row".
 * <p>
 * Access to the current row is done by subtyping this interface and adding accessor methods. If no call to
 * {@link #next()} has been done, or if it returned false, then such accessor methods throw {@link
 * IllegalStateException}.
 */
public interface RawCursor<T, EXCEPTION extends Exception> extends Supplier<T>, AutoCloseable
{
    /**
     * Move the cursor to the next row.
     * Return false if there are no more valid positions, generally indicating that the end of the data structure
     * has been reached.
     */
    boolean next() throws EXCEPTION;

    /**
     * Signal that the cursor is no longer needed.
     */
    @Override
    void close() throws EXCEPTION;

    default void forAll( Consumer<T> consumer ) throws EXCEPTION
    {
        mapForAll( Function.identity(), consumer );
    }

    default <R extends PrimitiveIntSet> R collect( R set, ToIntFunction<T> map ) throws EXCEPTION
    {
        try
        {
            while ( next() )
            {
                set.add( map.applyAsInt( get() ) );
            }
            return set;
        }
        finally
        {
            close();
        }
    }

    default <R, E> E mapReduce( E initialValue, Function<T,R> map, BiFunction<R,E,E> reduce ) throws EXCEPTION
    {
        try
        {
            E current = initialValue;
            while ( next() )
            {
                current = reduce.apply( map.apply( get() ), current );
            }
            return current;
        }
        finally
        {
            close();
        }
    }

    default <E> void mapForAll( Function<T,E> function, Consumer<E> consumer ) throws EXCEPTION
    {
        try
        {
            while ( next() )
            {
                consumer.accept( function.apply( get() ) );
            }
        }
        finally
        {
            close();
        }
    }

    default boolean exists() throws EXCEPTION
    {
        try
        {
            return next();
        }
        finally
        {
            close();
        }
    }

    default int count() throws EXCEPTION
    {
        try
        {
            int count = 0;
            while( next() )
            {
                count++;
            }
            return count;
        }
        finally
        {
            close();
        }
    }
}
