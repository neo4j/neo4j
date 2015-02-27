/**
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
package org.neo4j.helpers.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.neo4j.function.Function;
import org.neo4j.function.Function2;
import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.helpers.Predicate;

/**
 * {@link Iterable} wrappers around all available {@link Iterator iterators} found in {@link NewIterators}.
 *
 * @author Mattias Persson
 * @see NewIterators
 */
public abstract class NewIterables
{
    private NewIterables()
    {   // Singleton
    }

    // Array
    @SafeVarargs
    public static <T> Iterable<T> iterable( final T... items )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.iterator( items );
            }
        };
    }

    @SafeVarargs
    public static <T> Iterable<T> reversed( final T... items )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.reversed( items );
            }
        };
    }

    // Caching
    public static interface ListIterable<T> extends Iterable<T>
    {
        @Override
        ListIterator<T> iterator();
    }

    public static <T> ListIterable<T> caching( final Iterable<T> source )
    {
        return new ListIterable<T>()
        {
            private final List<T> visited = new ArrayList<>();

            @Override
            public ListIterator<T> iterator()
            {
                return new NewIterators.CachingIterator<>( source.iterator(), visited );
            }
        };
    }

    // Catching
    public static <T> Iterable<T> catching( Iterable<T> source, final Predicate<Throwable> catchAndIgnoreException )
    {
        return new CatchingIterable<T>( source )
        {
            @Override
            protected boolean exceptionOk( Throwable t )
            {
                return catchAndIgnoreException.accept( t );
            }
        };
    }

    public static abstract class CatchingIterable<T> implements Iterable<T>
    {
        private final Iterable<T> source;

        public CatchingIterable( Iterable<T> source )
        {
            this.source = source;
        }

        @Override
        public Iterator<T> iterator()
        {
            return new NewIterators.CatchingIterator<T>( source.iterator() )
            {
                @Override
                protected boolean exceptionOk( Throwable t )
                {
                    return CatchingIterable.this.exceptionOk( t );
                }
            };
        }

        protected abstract boolean exceptionOk( Throwable t );
    }

    // Concat
    public static <T> Iterable<T> concat( final Iterable<Iterable<T>> iterables )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.concatIterables( iterables.iterator() );
            }
        };
    }

    public static <T> Iterable<T> prepend( final T item, final Iterable<T> source )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.prepend( item, source.iterator() );
            }
        };
    }

    public static <T> Iterable<T> append( final Iterable<T> source, final T item )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.append( source.iterator(), item );
            }
        };
    }

    // Interleaving
    public static <T> Iterable<T> interleave( final Iterable<Iterable<T>> iterables )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new NewIterators.InterleavingIterator<>(
                        new MappingIterable<Iterable<T>,Iterator<T>>( iterables )
                {
                    @Override
                    protected Iterator<T> map( Iterable<T> item )
                    {
                        return item.iterator();
                    }
                } );
            }
        };
    }

    // Filtering
    public static <T> Iterable<T> filter( Iterable<T> source, final Predicate<T> filter )
    {
        return new FilteringIterable<T>( source )
        {
            @Override
            public boolean accept( T item )
            {
                return filter.accept( item );
            }
        };
    }

    public static <T> Iterable<T> dedup( final Iterable<T> source )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.dedup( source.iterator() );
            }
        };
    }

    public static <T> Iterable<T> skip( final Iterable<T> source, final int skipTheFirstNItems )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.skip( source.iterator(), skipTheFirstNItems );
            }
        };
    }

    public static abstract class FilteringIterable<T> implements Iterable<T>, Predicate<T>
    {
        private final Iterable<T> source;

        public FilteringIterable( Iterable<T> source )
        {
            this.source = source;
        }

        @Override
        public Iterator<T> iterator()
        {
            return new NewIterators.FilteringIterator<T>( source.iterator() )
            {
                @Override
                public boolean accept( T item )
                {
                    return FilteringIterable.this.accept( item );
                }
            };
        }

        @Override
        public abstract boolean accept( T item );
    }

    // Limiting
    public static <T> Iterable<T> limit( final Iterable<T> source, final int maxItems )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.limit( source.iterator(), maxItems );
            }
        };
    }

    // Mapping
    public static abstract class MappingIterable<FROM,TO> implements Iterable<TO>
    {
        private final Iterable<FROM> source;

        public MappingIterable( Iterable<FROM> source )
        {
            this.source = source;
        }

        @Override
        public Iterator<TO> iterator()
        {
            return new NewIterators.MappingIterator<FROM,TO>( source.iterator() )
            {
                @Override
                protected TO map( FROM item )
                {
                    return MappingIterable.this.map( item );
                }
            };
        }

        protected abstract TO map( FROM item );
    }

    // Nested
    public static <FROM,TO> Iterable<TO> nested( Iterable<FROM> source, final Function<FROM,Iterator<TO>> nester )
    {
        return new NestedIterable<FROM, TO>( source )
        {
            @Override
            protected Iterator<TO> nested( FROM item )
            {
                return nester.apply( item );
            }
        };
    }

    public static <FROM,TO> Iterable<TO> nestedIterables( Iterable<FROM> source,
            final Function<FROM,Iterable<TO>> nester )
    {
        return new NestedIterable<FROM, TO>( source )
        {
            @Override
            protected Iterator<TO> nested( FROM item )
            {
                return nester.apply( item ).iterator();
            }
        };
    }

    public static abstract class NestedIterable<FROM,TO> implements Iterable<TO>
    {
        private final Iterable<FROM> source;

        public NestedIterable( Iterable<FROM> source )
        {
            this.source = source;
        }

        @Override
        public Iterator<TO> iterator()
        {
            return new NewIterators.NestedIterator<FROM,TO>( source.iterator() )
            {
                @Override
                protected Iterator<TO> nested( FROM item )
                {
                    return NestedIterable.this.nested( item );
                }
            };
        }

        protected abstract Iterator<TO> nested( FROM item );
    }

    // Casting
    public static <FROM,TO> Iterable<TO> cast( final Iterable<FROM> source, final Class<TO> toClass )
    {
        return new Iterable<TO>()
        {
            @Override
            public Iterator<TO> iterator()
            {
                return NewIterators.cast( source.iterator(), toClass );
            }
        };
    }

    // Range
    public static Iterable<Integer> intRange( int end )
    {
        return intRange( 0, end );
    }

    public static Iterable<Integer> intRange( int start, int end )
    {
        return intRange( start, end, 1 );
    }

    public static Iterable<Integer> intRange( int start, int end, int stride )
    {
        return new RangeIterable<>( start, end, stride,
                NewIterators.INTEGER_STRIDER, NewIterators.INTEGER_COMPARATOR );
    }

    public static class RangeIterable<T,S> implements Iterable<T>
    {
        private final T start;
        private final T end;
        private final S stride;
        private final Function2<T, S, T> strider;
        private final Comparator<T> comparator;

        public RangeIterable( T start, T end, S stride, Function2<T, S, T> strider, Comparator<T> comparator )
        {
            this.start = start;
            this.end = end;
            this.stride = stride;
            this.strider = strider;
            this.comparator = comparator;
        }

        @Override
        public Iterator<T> iterator()
        {
            return new NewIterators.RangeIterator<>( start, end, stride, strider, comparator );
        }
    }

    // Singleton
    public static <T> Iterable<T> singleton( final T item )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.singleton( item );
            }
        };
    }

    // Cloning
    public static <T extends CloneableInPublic> Iterable<T> cloning( final Iterable<T> items,
            final Class<T> itemClass )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return NewIterators.cloning( items.iterator(), itemClass );
            }
        };
    }

    // Reversed
    public static <T> Iterable<T> reversed( Iterable<T> source )
    {
        List<T> allItems = new ArrayList<>();
        for ( Iterator<T> iterator = source.iterator(); iterator.hasNext(); )
        {
            allItems.add( iterator.next() );
        }
        Collections.reverse( allItems );
        return allItems;
    }

    // === Operations ===
    public static <T> T first( Iterable<T> iterable )
    {
        return NewIterators.first( iterable.iterator() );
    }

    public static <T> T first( Iterable<T> iterable, T defaultItem )
    {
        return NewIterators.first( iterable.iterator(), defaultItem );
    }

    public static <T> T last( Iterable<T> iterable )
    {
        return NewIterators.last( iterable.iterator() );
    }

    public static <T> T last( Iterable<T> iterable, T defaultItem )
    {
        return NewIterators.last( iterable.iterator(), defaultItem );
    }

    public static <T> T single( Iterable<T> iterable )
    {
        return NewIterators.single( iterable.iterator() );
    }

    public static <T> T single( Iterable<T> iterable, T defaultItem )
    {
        return NewIterators.single( iterable.iterator(), defaultItem );
    }

    public static <T> T itemAt( Iterable<T> iterable, int index )
    {
        return NewIterators.itemAt( iterable.iterator(), index );
    }

    public static <T> T itemAt( Iterable<T> iterable, int index, T defaultItem )
    {
        return NewIterators.itemAt( iterable.iterator(), index, defaultItem );
    }

    public static <T> int indexOf( T item, Iterable<T> iterable )
    {
        return NewIterators.indexOf( item, iterable.iterator() );
    }

    public static boolean equals( Iterable<?> first, Iterable<?> other )
    {
        return NewIterators.equals( first.iterator(), other.iterator() );
    }

    public static <C extends Collection<T>,T> C addToCollection( Iterable<T> iterable, C collection,
            Function2<T, C, Void> adder )
    {
        return NewIterators.addToCollection( iterable.iterator(), collection, adder );
    }

    public static <C extends Collection<T>,T> C addToCollection( Iterable<T> iterable, C collection )
    {
        return NewIterators.addToCollection( iterable.iterator(), collection );
    }

    public static <C extends Collection<T>,T> C addToCollectionAssertChanged( Iterable<T> iterable, C collection )
    {
        return NewIterators.addToCollectionAssertChanged( iterable.iterator(), collection );
    }

    public static <T> List<T> asList( Iterable<T> items )
    {
        return NewIterators.asList( items.iterator() );
    }

    public static <T> Set<T> asSet( Iterable<T> items )
    {
        return NewIterators.asSet( items.iterator() );
    }

    public static <T> Set<T> asSetAllowDuplicates( Iterable<T> items )
    {
        return NewIterators.asSetAllowDuplicates( items.iterator() );
    }

    public static int count( Iterable<?> iterable )
    {
        return NewIterators.count( iterable.iterator() );
    }

    public static <T> T[] asArray( Iterable<T> items, Class<T> itemClass )
    {
        return NewIterators.asArray( items.iterator(), itemClass );
    }
}
