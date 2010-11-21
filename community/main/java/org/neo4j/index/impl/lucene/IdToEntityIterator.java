/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.CatchingIteratorWrapper;
import org.neo4j.helpers.collection.IteratorUtil;

public abstract class IdToEntityIterator<T extends PropertyContainer>
        extends CatchingIteratorWrapper<T, Long> implements IndexHits<T>
{
    private final IndexHits<Long> ids;
    private final Set<Long> alreadyReturned = new HashSet<Long>();
    private boolean isExhausted;
    private IndexSearcherRef searcher;
    
    public IdToEntityIterator( IndexHits<Long> ids, IndexSearcherRef searcher )
    {
        super( ids );
        this.ids = ids;
        this.searcher = searcher;
    }
    
    @Override
    protected boolean exceptionOk( Throwable t )
    {
        return t instanceof NotFoundException;
    }
    
    @Override
    protected Long fetchNextOrNullFromSource( Iterator<Long> source )
    {
        while ( source.hasNext() )
        {
            Long id = source.next();
            if ( alreadyReturned.add( id ) )
            {
                return id;
            }
        }
        isExhausted = true;
        return null;
    }
    
    public float currentScore()
    {
        return this.ids.currentScore();
    }

    public int size()
    {
        return this.ids.size();
    }

    public Iterator<T> iterator()
    {
        return this;
    }

    public void close()
    {
        // Don't call close on the searcherRef if we've exhausted this iterator,
        // because by then the underlying DocToIdIterator have already closed it.
        if ( searcher != null && !isExhausted )
        {
            searcher.closeStrict();
            searcher = null;
        }
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
}
