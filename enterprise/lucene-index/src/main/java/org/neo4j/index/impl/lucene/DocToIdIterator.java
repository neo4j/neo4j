/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.neo4j.graphdb.index.IndexHits;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

class DocToIdIterator extends AbstractIndexHits<Long>
{
    private final Collection<Long> exclude;
    private IndexSearcherRef searcherOrNull;
    private final IndexHits<Document> source;
    private final Queue<Long> spool = new LinkedList<Long>();
    private int served = 0;
    private boolean closed = false;
    
    DocToIdIterator( IndexHits<Document> source, Collection<Long> exclude, IndexSearcherRef searcherOrNull )
    {
        this.source = source;
        this.exclude = exclude;
        this.searcherOrNull = searcherOrNull;
        if ( source.size() == 0 )
        {
            close();
        }
    }

    @Override
    protected Long fetchNextOrNull()
    {
        Long result = getNextOrNullFromSource();

        if(spool.size()>0)
        {
            return spool.poll();
        }

        if(result != null)
        {
            served++;
        }
        return result;
    }

    private Long getNextOrNullFromSource()
    {
        Long result = null;
        while ( result == null )
        {
            if ( !source.hasNext() )
            {
                endReached();
                break;
            }
            Document doc = source.next();
            Long id = Long.parseLong( doc.getField( LuceneIndex.KEY_DOC_ID ).stringValue() );
            if ( exclude == null || !exclude.contains( id ) )
            {
                result = id;
            }
        }
        return result;
    }

    protected void endReached()
    {
        closed=true;
        close();
    }
    
    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            this.searcherOrNull.closeStrict();
            this.searcherOrNull = null;
        }
    }

    public int size()
    {
        if( closed )
        {
            return served;
        }


        Long result = getNextOrNullFromSource();
        while(result!=null) {
            spool.add( result );
            served++;
            result = getNextOrNullFromSource();
        }

        return served;
    }

    private boolean isClosed()
    {
        return searcherOrNull==null;
    }

    public float currentScore()
    {
        return source.currentScore();
    }
    
    @Override
    protected void finalize() throws Throwable
    {
        close();
        super.finalize();
    }
}
