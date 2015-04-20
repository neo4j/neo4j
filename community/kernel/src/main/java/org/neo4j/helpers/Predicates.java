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
package org.neo4j.helpers;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.collection.Iterables;

/**
 * Common predicates
 */
public class Predicates
{
    public static <T> Predicate<T> TRUE()
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T instance )
            {
                return true;
            }
        };
    }

    public static <T> Predicate<T> not( final Predicate<T> specification )
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T instance )
            {
                return !specification.accept( instance );
            }
        };
    }

    public static <T> AndPredicate<T> and( final Predicate<T>... predicates )
    {
        return and( Arrays.asList( predicates ) );
    }

    public static <T> AndPredicate<T> and( final Iterable<Predicate<T>> predicates )
    {
        return new AndPredicate<T>( predicates );
    }

    public static <T> OrPredicate<T> or( final Predicate<T>... predicates )
    {
        return or( Arrays.asList( predicates ) );
    }

    public static <T> OrPredicate<T> or( final Iterable<Predicate<T>> predicates )
    {
        return new OrPredicate<T>( predicates );
    }

    public static <T> Predicate<T> equalTo( final T allowed )
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T item )
            {
                return allowed == null ? item == null : allowed.equals( item );
            }
        };
    }

    public static <T> Predicate<T> in( final T... allowed )
    {
        return in( Arrays.asList( allowed ) );
    }

    public static <T> Predicate<T> in( final Iterable<T> allowed )
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T item )
            {
                for ( T allow : allowed )
                {
                    if ( allow.equals( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static <T> Predicate<T> in( final Collection<T> allowed )
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T item )
            {
                return allowed.contains( item );
            }
        };
    }

    @SuppressWarnings( "rawtypes" )
    private static Predicate NOT_NULL = new Predicate()
    {
        @Override
        public boolean accept( Object item )
        {
            return item != null;
        }
    };

    @SuppressWarnings( "unchecked" )
    public static <T> Predicate<T> notNull()
    {
        return NOT_NULL;
    }

    public static <FROM, TO> Predicate<FROM> translate( final Function<FROM, TO> function,
                                                        final Predicate<? super TO> specification )
    {
        return new Predicate<FROM>()
        {
            @Override
            public boolean accept( FROM item )
            {
                return specification.accept( function.apply( item ) );
            }
        };
    }

    public static <TYPE> void await( Provider<TYPE> provider, Predicate<TYPE> predicate, long timeout, TimeUnit unit )
            throws TimeoutException, InterruptedException
    {
        long sleep = Math.max( unit.toMillis( timeout ) / 100, 1 );
        long deadline = System.currentTimeMillis() + unit.toMillis( timeout );
        do
        {
            if ( predicate.accept( provider.instance() ) )
            {
                return;
            }
            Thread.sleep( sleep );
        }
        while ( System.currentTimeMillis() < deadline );
        throw new TimeoutException( "Waited for " + timeout + " " + unit + ", but " + predicate + " was not accepted." );
    }

    public static class AndPredicate<T> implements Predicate<T>
    {
        private final Iterable<Predicate<T>> predicates;

        private AndPredicate( Iterable<Predicate<T>> predicates )
        {
            this.predicates = predicates;
        }

        @Override
        public boolean accept( T instance )
        {
            for ( Predicate<T> specification : predicates )
            {
                if ( !specification.accept( instance ) )
                {
                    return false;
                }
            }

            return true;
        }

        public AndPredicate<T> and( Predicate<T>... predicates )
        {
            Iterable<Predicate<T>> iterable = Iterables.iterable( predicates );
            Iterable<Predicate<T>> flatten = Iterables.flatten( this.predicates, iterable );
            return Predicates.and( flatten );
        }

        public OrPredicate<T> or( Predicate<T>... predicates )
        {
            return Predicates.or( Iterables.prepend( this, Arrays.asList( predicates ) ) );
        }
    }

    public static class OrPredicate<T> implements Predicate<T>
    {
        private final Iterable<Predicate<T>> predicates;

        private OrPredicate( Iterable<Predicate<T>> predicates )
        {
            this.predicates = predicates;
        }

        @Override
        public boolean accept( T instance )
        {
            for ( Predicate<T> specification : predicates )
            {
                if ( specification.accept( instance ) )
                {
                    return true;
                }
            }

            return false;
        }

        public AndPredicate<T> and( Predicate<T>... predicates )
        {
            return Predicates.and( Iterables.prepend( this, Arrays.asList( predicates ) ) );
        }

        public OrPredicate<T> or( Predicate<T>... predicates )
        {
            Iterable<Predicate<T>> iterable = Iterables.iterable( predicates );
            Iterable<Predicate<T>> flatten = Iterables.flatten( this.predicates, iterable );
            return Predicates.or( flatten );
        }
    }

    public static Predicate<String> stringContains( final String string )
    {
        return new Predicate<String>()
        {
            @Override
            public boolean accept( String item )
            {
                return item.contains( string );
            }
        };
    }

    public static <T> Predicate<T> instanceOf( final Class clazz)
    {
        return new Predicate<T>()
        {
            @Override
            public boolean accept( T item )
            {
                return item != null && clazz.isInstance( item );
            }
        };
    }
}
