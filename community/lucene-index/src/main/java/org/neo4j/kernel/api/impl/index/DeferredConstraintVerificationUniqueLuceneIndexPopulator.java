/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.util.FailureStorage;

import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;

class DeferredConstraintVerificationUniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    private SearcherManager searcherManager;

    DeferredConstraintVerificationUniqueLuceneIndexPopulator( LuceneDocumentStructure documentStructure,
                                                              LuceneIndexWriterFactory indexWriterFactory,
                                                              IndexWriterStatus writerStatus,
                                                              DirectoryFactory dirFactory, File dirFile,
                                                              FailureStorage failureStorage, long indexId )
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile, failureStorage, indexId );
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
    }

    @Override
    protected void flush() throws IOException
    {
        // no need to do anything yet.
    }

    @Override
    public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
    {
        writer.addDocument( documentStructure.newDocumentRepresentingProperty( nodeId, propertyValue ) );
    }

    @Override
    public void verifyDeferredConstraints() throws IndexEntryConflictException, IOException
    {
        searcherManager.maybeRefresh();
        try ( IndexSearcher searcher = searcherManager.acquire() )
        {
            IndexReader indexReader = searcher.getIndexReader();

            TermEnum terms = indexReader.terms();
            while ( terms.next() )
            {
                Term term = terms.term();

                if ( !NODE_ID_KEY.equals( term.field() ) && terms.docFreq() > 1 )
                {
                    TopDocs duplicateEntries = searcher.search( new TermQuery( term ), 2 );

                    // docFreq may include deleted documents, so we are not yet certain that this term is a duplicate
                    if ( duplicateEntries.totalHits > 1 )
                    {
                        long nodeId1 = getNodeId( indexReader, duplicateEntries, 0 );
                        long nodeId2 = getNodeId( indexReader, duplicateEntries, 1 );
                        Object propertyValue = documentStructure.propertyValue( term );
                        throw new PreexistingIndexEntryConflictException( propertyValue, nodeId1, nodeId2 );
                    }
                }
            }
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater() throws IOException
    {
        return new IndexUpdater()
        {
            List<Object> updatedPropertyValues = new ArrayList<>();

            @Override
            public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
            {
                long nodeId = update.getNodeId();
                switch ( update.getUpdateMode() )
                {
                    case ADDED:
                    case CHANGED:
                        // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                        writer.updateDocument( documentStructure.newQueryForChangeOrRemove( nodeId ),
                                documentStructure.newDocumentRepresentingProperty( nodeId,
                                        update.getValueAfter() ) );
                        updatedPropertyValues.add( update.getValueAfter() );
                        break;
                    case REMOVED:
                        writer.deleteDocuments( documentStructure.newQueryForChangeOrRemove( nodeId ) );
                        break;
                    default:
                        throw new IllegalStateException( "Unknown update mode " + update.getUpdateMode() );
                }
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
                searcherManager.maybeRefresh();
                IndexSearcher searcher = searcherManager.acquire();
                try
                {
                    for ( Object propertyValue : updatedPropertyValues )
                    {
                        TopDocs docs = searcher.search( documentStructure.newQuery( propertyValue ), 2 );
                        if ( docs.totalHits > 1 )
                        {
                            long nodeId1 = getNodeId( searcher.getIndexReader(), docs, 0 );
                            long nodeId2 = getNodeId( searcher.getIndexReader(), docs, 1 );
                            throw new PreexistingIndexEntryConflictException( propertyValue, nodeId1, nodeId2 );
                        }
                    }
                }
                finally
                {
                    searcherManager.release( searcher );
                }
            }

            @Override
            public void remove( Iterable<Long> nodeIds )
            {
                throw new UnsupportedOperationException( "should not remove() from populating index" );
            }
        };
    }

    private long getNodeId( IndexReader indexReader, TopDocs duplicateEntries, int index ) throws IOException
    {
        return documentStructure.getNodeId( indexReader.document( duplicateEntries.scoreDocs[index].doc ) );
    }
}
