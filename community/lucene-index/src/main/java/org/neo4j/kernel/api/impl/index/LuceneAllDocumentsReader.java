/*
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;

public class LuceneAllDocumentsReader implements BoundedIterable<Document>
{
    private final IndexSearcher searcher;
    private final LuceneIndexAccessor.LuceneReferenceManager<IndexSearcher> searcherManager;

    public LuceneAllDocumentsReader(
            LuceneIndexAccessor.LuceneReferenceManager<IndexSearcher> searcherManager )
    {
        this.searcherManager = searcherManager;
        try
        {
            this.searcher = searcherManager.acquire();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
            private DocIdSetIterator disi = iterateAllDocs();

            @Override
            protected Document fetchNextOrNull()
            {
                try
                {
                    int doc = disi.nextDoc();
                    if ( doc == DocIdSetIterator.NO_MORE_DOCS )
                    {
                        return null;
                    }
                    return getDocument( doc );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
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

    private DocIdSetIterator iterateAllDocs()
    {
        IndexReader reader = searcher.getIndexReader();
        final Bits liveDocs = MultiFields.getLiveDocs( reader );
        final DocIdSetIterator allDocs = DocIdSetIterator.all( reader.maxDoc() );
        if ( liveDocs == null )
        {
            return allDocs;
        }

        return new FilteredDocIdSetIterator( allDocs )
        {
            @Override
            protected boolean match( int doc )
            {
                return liveDocs.get( doc );
            }
        };
    }

    private int maxDocIdBoundary()
    {
        return searcher.getIndexReader().maxDoc();
    }
}
