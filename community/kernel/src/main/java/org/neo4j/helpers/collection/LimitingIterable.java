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
 * Limits the amount of items returned by an {@link Iterable}, or rather
 * {@link Iterator}s spawned from it.
 * 
 * @author Mattias Persson
 *
 * @param <T> the type of items in this {@link Iterable}.
 * @see LimitingIterator
 */
public class LimitingIterable<T> implements Iterable<T>
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
    public LimitingIterable( Iterable<T> source, int limit )
    {
        this.source = source;
        this.limit = limit;
    }
    
    @Override
    public Iterator<T> iterator()
    {
        return new LimitingIterator<T>( source.iterator(), limit );
    }
}
