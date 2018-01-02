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
package org.neo4j.helpers.collection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criteria pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterable<T> implements Iterable<T>
{
	private final Iterable<T> source;
	private final Predicate<T> predicate;

    /**
     * @deprecated use {@link #FilteringIterable(Iterable, Predicate)} instead
     * @param source iterable to fetch items from
     * @param predicate filter to decide which items to pass through
     */
    @Deprecated
	public FilteringIterable( Iterable<T> source, org.neo4j.helpers.Predicate<T> predicate )
	{
        this( source, org.neo4j.helpers.Predicates.upgrade( predicate ) );
    }

    public FilteringIterable( Iterable<T> source, Predicate<T> predicate )
    {
        this.source = source;
        this.predicate = predicate;
    }

	public Iterator<T> iterator()
	{
		return new FilteringIterator<T>( source.iterator(), predicate );
	}

    public static <T> Iterable<T> notNull( Iterable<T> source )
    {
        return new FilteringIterable<T>( source, Predicates.<T>notNull() );
    }
    
    public static <T> Iterable<T> noDuplicates( Iterable<T> source )
    {
        return new FilteringIterable<T>( source, Predicates.<T>noDuplicates() );
    }

    /**
     * @deprecated use {@link Predicates#noDuplicates()} instead
     * @param <T> the type of the elements
     * @return a filter which skips duplicates
     */
    @Deprecated
    public static <T> org.neo4j.helpers.Predicate<T> noDuplicatesPredicate()
    {
        return new org.neo4j.helpers.Predicate<T>()
        {
            private final Set<T> visitedItems = new HashSet<T>();
            
            public boolean accept( T item )
            {
                return visitedItems.add( item );
            }
        };
    }

    /**
     * @deprecated use {@link Predicates#notNull()} instead
     * @param <T> the type of the elements
     * @return a filter which skips {@code null}s
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public static <T> org.neo4j.helpers.Predicate<T> notNullPredicate()
    {
        return (org.neo4j.helpers.Predicate<T>) NOT_NULL_PREDICATE;
    }
    
    @SuppressWarnings("unchecked")
    private static final org.neo4j.helpers.Predicate NOT_NULL_PREDICATE = new org.neo4j.helpers.Predicate()
    {
        public boolean accept( Object item )
        {
            return item != null;
        }
    };
}
