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

import java.util.Iterator;

import org.neo4j.helpers.Predicate;

/**
 * An iterator which filters another iterator, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterator<T> extends PrefetchingIterator<T>
{
	private final Iterator<T> source;
	private final Predicate<T> predicate;
	
	public FilteringIterator( Iterator<T> source, Predicate<T> predicate )
	{
		this.source = source;
		this.predicate = predicate;
	}
	
	@Override
	protected T fetchNextOrNull()
	{
		while ( source.hasNext() )
		{
			T testItem = source.next();
			if ( predicate.accept( testItem ) )
			{
				return testItem;
			}
		}
		return null;
	}

    public static <T> Iterator<T> notNull( Iterator<T> source )
    {
        return new FilteringIterator<T>( source, FilteringIterable.<T>notNullPredicate() );
    }
    
    public static <T> Iterator<T> noDuplicates( Iterator<T> source )
    {
        return new FilteringIterator<T>( source, FilteringIterable.<T>noDuplicatesPredicate() );
    }
}
