/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

/**
 * Limits the amount of items returned by an {@link ResourceIterable}, or rather
 * {@link ResourceIterator}s spawned from it.
 *
 * @param <T> the type of items in this {@link Iterable}.
 * @see LimitingResourceIterator
 */
public class LimitingResourceIterable<T> implements ResourceIterable<T>
{
    private final Iterable<T> source;
    private final int limit;

    /**
     * Instantiates a new limiting {@link Iterable} which can limit the number
     * of items returned from iterators it spawns.
     *
     * @param source the source of items.
     * @param limit the limit, i.e. the max number of items to return.
     */
    public LimitingResourceIterable( ResourceIterable<T> source, int limit )
    {
        this.source = source;
        this.limit = limit;
    }

    @Override
    public ResourceIterator<T> iterator()
    {
        return new LimitingResourceIterator<>( Iterators.asResourceIterator( source.iterator() ), limit );
    }
}
