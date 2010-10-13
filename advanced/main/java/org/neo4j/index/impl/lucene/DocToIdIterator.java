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

import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.neo4j.helpers.collection.PrefetchingIterator;

class DocToIdIterator extends PrefetchingIterator<Long>
{
    private final SearchResult searchResult;
    private final Collection<Long> exclude;
    private final IndexSearcherRef searcherOrNull;
    
    DocToIdIterator( SearchResult searchResult, Collection<Long> exclude,
        IndexSearcherRef searcherOrNull )
    {
        this.searchResult = searchResult;
        this.exclude = exclude;
        this.searcherOrNull = searcherOrNull;
    }

    @Override
    protected Long fetchNextOrNull()
    {
        Long result = null;
        while ( result == null )
        {
            if ( !searchResult.documents.hasNext() )
            {
                endReached();
                break;
            }
            Document doc = searchResult.documents.next();
            long id = Long.parseLong(
                doc.getField( LuceneIndex.KEY_DOC_ID ).stringValue() );
            if ( exclude == null || !exclude.contains( id ) )
            {
                result = id;
            }
        }
        return result;
    }
    
    private void endReached()
    {
        if ( this.searcherOrNull != null )
        {
            this.searcherOrNull.closeStrict();
        }
    }

    public int size()
    {
        return searchResult.size;
    }

    public void close()
    {
    }

    public Iterator<Long> iterator()
    {
        return this;
    }
}
