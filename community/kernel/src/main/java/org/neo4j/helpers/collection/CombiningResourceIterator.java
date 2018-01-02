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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;

public class CombiningResourceIterator<T> extends CombiningIterator<T> implements ResourceIterator<T>
{
    private final Iterator<ResourceIterator<T>> iterators;
    private final Collection<ResourceIterator<T>> seenIterators = new ArrayList<>();
    private ResourceIterator<T> currentIterator;

    public CombiningResourceIterator( Iterator<ResourceIterator<T>> iterators )
    {
        super(iterators);
        this.iterators = iterators;
    }

    @Override
    protected Iterator<T> nextIteratorOrNull()
    {
        if(iterators.hasNext())
        {
            currentIterator = iterators.next();
            seenIterators.add( currentIterator );
            return currentIterator;
        }
        return null;
    }

    @Override
    public void close()
    {
        for ( ResourceIterator<T> seenIterator : seenIterators )
        {
            seenIterator.close();
        }

        while(iterators.hasNext())
        {
            iterators.next().close();
        }
    }
}
