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
 * Concatenates sub-iterables of an iterable.
 *
 * @see NestingIterator
 * 
 * @param <T> the type of items.
 * @param <U> the type of items in the surface item iterator
 */
public abstract class NestingIterable<T, U> implements Iterable<T>
{
	private final Iterable<U> source;

	public NestingIterable( Iterable<U> source )
	{
		this.source = source;
	}

	public Iterator<T> iterator()
	{
		return new NestingIterator<T, U>( source.iterator() )
		{
			@Override
			protected Iterator<T> createNestedIterator( U item )
			{
				return NestingIterable.this.createNestedIterator( item );
			}
		};
	}

	protected abstract Iterator<T> createNestedIterator( U item );
}
