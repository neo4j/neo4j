/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.index.AllEntriesIndexReader;

public class LuceneAllEntriesIndexAccessorReader implements AllEntriesIndexReader
{
    private final IndexSearcher searcher;
    private final LuceneDocumentStructure documentLogic;
    private final ReferenceManager<IndexSearcher> searcherManager;

    public LuceneAllEntriesIndexAccessorReader(
            ReferenceManager<IndexSearcher> searcherManager, LuceneDocumentStructure documentLogic )
    {
        this.searcherManager = searcherManager;
        this.searcher = searcherManager.acquire();
        this.documentLogic = documentLogic;
    }

    @Override
    public long maxCount()
    {
        return maxDocIdBoundary();
    }

    @Override
    public Iterator<Long> iterator()
    {
        return new PrefetchingIterator<Long>()
        {
            private int docId;

            @Override
            protected Long fetchNextOrNull()
            {
                Long node = null;
                while ( node == null && isPossibleDocId( docId ) )
                {
                    if ( ! deleted( docId ) )
                    {
                        node = getNode( docId );
                    }
                    docId++;
                }
                return node;
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

    private Long getNode( int docId )
    {
        try
        {
            return documentLogic.getNodeId( searcher.doc( docId ) );
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
