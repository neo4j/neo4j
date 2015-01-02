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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;

public class LuceneAllDocumentsReader implements BoundedIterable<Document>
{
    private final IndexSearcher searcher;
    private final ReferenceManager<IndexSearcher> searcherManager;

    public LuceneAllDocumentsReader(
            ReferenceManager<IndexSearcher> searcherManager )
    {
        this.searcherManager = searcherManager;
        this.searcher = searcherManager.acquire();
    }

    @Override
    public long maxCount()
    {
        return maxDocIdBoundary();
    }

    @Override
    public Iterator<Document> iterator()
    {
        return new PrefetchingIterator<Document>()
        {
            private int docId;

            @Override
            protected Document fetchNextOrNull()
            {
                Document document = null;
                while ( document == null && isPossibleDocId( docId ) )
                {
                    if ( ! deleted( docId ) )
                    {
                        document = getDocument( docId );
                    }
                    docId++;
                }
                return document;
            }
        };
    }

    @Override
    public void close()
    {
        try
        {
            searcherManager.release( searcher );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Document getDocument( int docId )
    {
        try
        {
            return searcher.doc( docId );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private boolean deleted( int docId )
    {
        return searcher.getIndexReader().isDeleted( docId );
    }

    private boolean isPossibleDocId( int docId )
    {
        return docId < maxDocIdBoundary();
    }

    private int maxDocIdBoundary()
    {
        return searcher.maxDoc();
    }
}
