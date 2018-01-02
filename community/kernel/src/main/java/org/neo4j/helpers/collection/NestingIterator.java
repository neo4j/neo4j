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

import java.util.Iterator;

/**
 * Concatenates sub-iterators of an iterator.
 * 
 * Iterates through each item in an iterator. For each item, the
 * {@link #createNestedIterator(Object)} is invoked to create a sub-iterator.
 * The resulting iterator iterates over each item in each sub-iterator. In
 * effect flattening the iteration.
 *
 * @param <T> the type of items to return
 * @param <U> the type of items in the surface item iterator
 */
public abstract class NestingIterator<T, U> extends PrefetchingIterator<T>
{
	private final Iterator<U> source;
	private Iterator<T> currentNestedIterator;
	private U currentSurfaceItem;

	public NestingIterator( Iterator<U> source )
	{
		this.source = source;
	}

	protected abstract Iterator<T> createNestedIterator( U item );

	public U getCurrentSurfaceItem()
	{
		if ( this.currentSurfaceItem == null )
		{
			throw new IllegalStateException( "Has no surface item right now," +
				" you must do at least one next() first" );
		}
		return this.currentSurfaceItem;
	}

	@Override
	protected T fetchNextOrNull()
	{
		if ( currentNestedIterator == null ||
			!currentNestedIterator.hasNext() )
		{
			while ( source.hasNext() )
			{
				currentSurfaceItem = source.next();
				currentNestedIterator =
					createNestedIterator( currentSurfaceItem );
				if ( currentNestedIterator.hasNext() )
				{
					break;
				}
			}
		}
		return currentNestedIterator != null &&
			currentNestedIterator.hasNext() ?
			currentNestedIterator.next() : null;
	}
}
