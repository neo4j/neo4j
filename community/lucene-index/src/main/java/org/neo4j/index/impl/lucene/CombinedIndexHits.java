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
package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.IteratorUtil;

public class CombinedIndexHits<T> extends CombiningIterator<T> implements IndexHits<T>
{
    private final Collection<IndexHits<T>> allIndexHits;
    private final int size;
    
    public CombinedIndexHits( Collection<IndexHits<T>> iterators )
    {
        super( iterators );
        this.allIndexHits = iterators;
        size = accumulatedSize( iterators );
    }

    private int accumulatedSize( Collection<IndexHits<T>> iterators )
    {
        int result = 0;
        for ( IndexHits<T> hits : iterators )
        {
            result += hits.size();
        }
        return result;
    }

    public IndexHits<T> iterator()
    {
        return this;
    }
    
    @Override
    protected IndexHits<T> currentIterator()
    {
        return (IndexHits<T>) super.currentIterator();
    }

    public int size()
    {
        return size;
    }

    public void close()
    {
        for ( IndexHits<T> hits : allIndexHits )
        {
            hits.close();
        }
        allIndexHits.clear();
    }

    public T getSingle()
    {
        try
        {
            return IteratorUtil.singleOrNull( (Iterator<T>) this );
        }
        finally
        {
            close();
        }
    }

    public float currentScore()
    {
        return currentIterator().currentScore();
    }
}
