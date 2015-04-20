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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.register.Register;

/**
 * @deprecated Use {@link DeferredConstraintVerificationUniqueLuceneIndexPopulator} instead.
 */
@Deprecated
class UniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    private static final float LOAD_FACTOR = 0.75f;
    private final int batchSize;
    private SearcherManager searcherManager;
    private Map<Object, Long> currentBatch = newBatchMap();

    UniqueLuceneIndexPopulator( int batchSize, LuceneDocumentStructure documentStructure,
                                IndexWriterFactory<LuceneIndexWriter> indexWriterFactory,
                                IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile,
                                FailureStorage failureStorage, long indexId )
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile, failureStorage, indexId );
        this.batchSize = batchSize;
    }

    private HashMap<Object, Long> newBatchMap()
    {
        return new HashMap<>( (int) (batchSize / LOAD_FACTOR), LOAD_FACTOR );
    }

    @Override
    public void create() throws IOException
    {
        super.create();
        searcherManager = writer.createSearcherManager();
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
    public void add( long nodeId, Object propertyValue )
            throws IndexEntryConflictException, IOException, IndexCapacityExceededException
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
            Fieldable encodedValue = documentStructure.encodeAsFieldable( propertyValue );
            writer.addDocument( documentStructure.newDocumentRepresentingProperty( nodeId, encodedValue ) );
            if ( currentBatch.size() >= batchSize )
            {
                startNewBatch();
            }
        }
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        // constraints are checked in add() so do nothing
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
    {
        return new IndexUpdater()
        {
            @Override
            public Reservation validate( Iterable<NodePropertyUpdate> updates ) throws IOException
            {
                return Reservation.EMPTY;
            }

            @Override
            public void process( NodePropertyUpdate update )
                    throws IOException, IndexEntryConflictException, IndexCapacityExceededException
            {
                add( update.getNodeId(), update.getValueAfter() );
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
            }

            @Override
            public void remove( PrimitiveLongSet nodeIds )
            {
                throw new UnsupportedOperationException( "should not remove() from populating index" );
            }
        };
    }

    @Override
    public long sampleResult( Register.DoubleLong.Out result )
    {
        throw new UnsupportedOperationException();
    }

    private void startNewBatch() throws IOException
    {
        searcherManager.maybeRefresh();
        currentBatch = newBatchMap();
    }
}
