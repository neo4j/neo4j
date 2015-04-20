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
package org.neo4j.helpers.collection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.Predicate;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterable<T> implements Iterable<T>
{
	private final Iterable<T> source;
	private final Predicate<T> predicate;

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
        return new FilteringIterable<T>( source, FilteringIterable.<T>notNullPredicate() );
    }
    
    public static <T> Iterable<T> noDuplicates( Iterable<T> source )
    {
        return new FilteringIterable<T>( source, FilteringIterable.<T>noDuplicatesPredicate() );
    }
    
    public static <T> Predicate<T> noDuplicatesPredicate()
    {
        return new Predicate<T>()
        {
            private final Set<T> visitedItems = new HashSet<T>();
            
            public boolean accept( T item )
            {
                return visitedItems.add( item );
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> notNullPredicate()
    {
        return (Predicate<T>) NOT_NULL_PREDICATE;
    }
    
    @SuppressWarnings("unchecked")
    private static final Predicate NOT_NULL_PREDICATE = new Predicate()
    {
        public boolean accept( Object item )
        {
            return item != null;
        }
    };
}
