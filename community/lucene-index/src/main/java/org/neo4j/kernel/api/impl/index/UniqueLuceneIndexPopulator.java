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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.util.FailureStorage;

class UniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    static final int DEFAULT_BATCH_SIZE = 1024;
    private static final float LOAD_FACTOR = 0.75f;
    private final int batchSize;
    private SearcherManager searcherManager;
    private Map<Object, Long> currentBatch = newBatchMap();

    UniqueLuceneIndexPopulator( int batchSize, LuceneDocumentStructure documentStructure,
                                LuceneIndexWriterFactory indexWriterFactory,
                                IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile,
                                FailureStorage failureStorage, long indexId )
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile, failureStorage, indexId );
        this.batchSize = batchSize;
    }

    private HashMap<Object, Long> newBatchMap()
    {
        return new HashMap<Object, Long>( (int) (batchSize / LOAD_FACTOR), LOAD_FACTOR );
    }

    @Override
    public void create() throws IOException
    {
        super.create();
        searcherManager = new SearcherManager( writer, true, new SearcherFactory() );
    }

    @Override
    public void drop()
    {
        currentBatch.clear();
    }

    @Override
    protected void flush() throws IOException
    {
        // no need to do anything yet.
    }

    @Override
    public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
    {
        Long previousEntry = currentBatch.get( propertyValue );
        if ( previousEntry == null )
        {
            IndexSearcher searcher = searcherManager.acquire();
            try
            {
                TopDocs docs = searcher.search( documentStructure.newQuery( propertyValue ), 1 );
                if ( docs.totalHits > 0 )
                {
                    Document doc = searcher.getIndexReader().document( docs.scoreDocs[0].doc );
                    previousEntry = documentStructure.getNodeId( doc );
                }
            }
            finally
            {
                searcherManager.release( searcher );
            }
        }
        if ( previousEntry != null )
        {
            if ( previousEntry != nodeId )
            {
                throw new PreexistingIndexEntryConflictException( propertyValue, previousEntry, nodeId );
            }
        }
        else
        {
            currentBatch.put( propertyValue, nodeId );
            writer.addDocument( documentStructure.newDocument( nodeId, propertyValue ) );
            if ( currentBatch.size() >= batchSize )
            {
                startNewBatch();
            }
        }
    }

    private void startNewBatch() throws IOException
    {
        searcherManager.maybeRefresh();
        currentBatch = newBatchMap();
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            add( update.getNodeId(), update.getValueAfter() );
        }
    }
}
