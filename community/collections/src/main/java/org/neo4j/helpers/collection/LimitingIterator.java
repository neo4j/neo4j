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
 * Limits the amount of items returned by an {@link Iterator}.
 * 
 * @author Mattias Persson
 *
 * @param <T> the type of items in this {@link Iterator}.
 */
public class LimitingIterator<T> extends PrefetchingIterator<T>
{
    private int returned;
    private final Iterator<T> source;
    private final int limit;
    
    /**
     * Instantiates a new limiting iterator which iterates over {@code source}
     * and if {@code limit} items have been returned the next {@link #hasNext()}
     * will return {@code false}.
     * 
     * @param source the source of items.
     * @param limit the limit, i.e. the max number of items to return.
     */
    public LimitingIterator( Iterator<T> source, int limit )
    {
        this.source = source;
        this.limit = limit;
    }
    
    @Override
    protected T fetchNextOrNull()
    {
        if ( !source.hasNext() || returned >= limit )
            return null;
        try
        {
            return source.next();
        }
        finally
        {
            returned++;
        }
    }
    
    /**
     * @return {@code true} if the number of items returned up to this point
     * is equal to the limit given in the constructor, otherwise {@code false}.
     */
    public boolean limitReached()
    {
        return returned == limit;
    }
}
