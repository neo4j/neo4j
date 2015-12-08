/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.function;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Constructors for basic {@link Predicate} types
 */
public class Predicates
{
    private static final Predicate TRUE = item -> true;

    private static final Predicate FALSE = item -> false;

    private static final Predicate NOT_NULL = item -> item != null;

    @SuppressWarnings( "unchecked" )
    public static <T> Predicate<T> alwaysTrue()
    {
        return (Predicate<T>) TRUE;
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Predicate<T> alwaysFalse()
    {
        return (Predicate<T>) FALSE;
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Predicate<T> notNull()
    {
        return (Predicate<T>) NOT_NULL;
    }

    @SafeVarargs
    public static <T> Predicate<T> all( final Predicate<T>... predicates )
    {
        return all( Arrays.asList( predicates ) );
    }

    public static <T> Predicate<T> all( final Iterable<Predicate<T>> predicates )
    {
        return item -> {
            for ( Predicate<T> predicate : predicates )
            {
                if ( !predicate.test( item ) )
                {
                    return false;
                }
            }
            return true;
        };
    }

    @SafeVarargs
    public static <T> Predicate<T> any( final Predicate<T>... predicates )
    {
        return any( Arrays.asList( predicates ) );
    }

    public static <T> Predicate<T> any( final Iterable<Predicate<T>> predicates )
    {
        return item -> {
            for ( Predicate<T> predicate : predicates )
            {
                if ( predicate.test( item ) )
                {
                    return true;
                }
            }
            return false;
        };
    }

    public static <T> Predicate<T> instanceOf( final Class clazz )
    {
        return item -> item != null && clazz.isInstance( item );
    }

    public static <T> Predicate<T> instanceOfAny( final Class... classes )
    {
        return item -> {
            if ( item != null )
            {
                for ( Class clazz : classes )
                {
                    if ( clazz.isInstance( item ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        };
    }

    public static <T> Predicate<T> noDuplicates()
    {
        return new Predicate<T>()
        {
            private final Set<T> visitedItems = new HashSet<>();

            public boolean test( T item )
            {
                return visitedItems.add( item );
            }
        };
    }

    public static <TYPE> void await( Supplier<TYPE> supplier, Predicate<TYPE> predicate, long timeout, TimeUnit unit )
            throws TimeoutException, InterruptedException
    {
        await( Suppliers.compose( supplier, predicate ), timeout, unit );
    }

    public static void await( Supplier<Boolean> condition, long timeout, TimeUnit unit )
            throws TimeoutException, InterruptedException
    {
        long sleep = Math.max( unit.toMillis( timeout ) / 100, 1 );
        long deadline = System.currentTimeMillis() + unit.toMillis( timeout );
        do
        {
            if ( condition.get() )
            {
                return;
            }
            Thread.sleep( sleep );
        }
        while ( System.currentTimeMillis() < deadline );
        throw new TimeoutException( "Waited for " + timeout + " " + unit + ", but " + condition + " was not accepted." );
    }


    public static void awaitForever( BooleanSupplier condition, long checkInterval, TimeUnit unit ) throws InterruptedException
    {
        long sleep = unit.toMillis( checkInterval );
        do
        {
            if ( condition.getAsBoolean() )
            {
                return;
            }
            Thread.sleep( sleep );
        }
        while ( true );
    }

    public static <T> Predicate<T> in( final T... allowed )
    {
        return in( Arrays.asList( allowed ) );
    }

    public static <T> Predicate<T> in( final Iterable<T> allowed )
    {
        return item -> {
            for ( T allow : allowed )
            {
                if ( allow.equals( item ) )
                {
                    return true;
                }
            }
            return false;
        };
    }}
