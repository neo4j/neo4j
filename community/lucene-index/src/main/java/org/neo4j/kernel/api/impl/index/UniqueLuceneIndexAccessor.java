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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

class UniqueLuceneIndexAccessor extends LuceneIndexAccessor
{
    public UniqueLuceneIndexAccessor( LuceneDocumentStructure documentStructure,
                                      LuceneIndexWriterFactory indexWriterFactory, IndexWriterStatus writerStatus,
                                      DirectoryFactory dirFactory, File dirFile ) throws IOException
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile );
    }

    @Override
    public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException, IndexEntryConflictException
    {
        Map<Object, Long> valueToNodeIdInCurrentTx = new HashMap<Object, Long>();
        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                add( update.getNodeId(), update.getValueAfter(), valueToNodeIdInCurrentTx );
                break;
            case CHANGED:
                change( update.getNodeId(), update.getValueAfter() );
                break;
            case REMOVED:
                remove( update.getNodeId() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        // Call refresh here since we are guaranteed to be the only thread writing concurrently.
        searcherManager.maybeRefresh();
    }

    @Override
    public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                addRecovered( update.getNodeId(), update.getValueAfter() );
                break;
            case CHANGED:
                change( update.getNodeId(), update.getValueAfter() );
                break;
            case REMOVED:
                remove( update.getNodeId() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }
        searcherManager.maybeRefresh();
    }

    private void addRecovered( long nodeId, Object value ) throws IOException
    {
        IndexSearcher searcher = searcherManager.acquire();
        try
        {
            TopDocs hits = searcher.search( new TermQuery( documentStructure.newQueryForChangeOrRemove( nodeId ) ), 1 );
            if ( hits.totalHits > 0 )
            {
                writer.updateDocument( documentStructure.newQueryForChangeOrRemove( nodeId ),
                                       documentStructure.newDocument( nodeId, value ) );
            }
            else
            {
                try
                {
                    add( nodeId, value, new HashMap<Object, Long>() );
                }
                catch ( IndexEntryConflictException e )
                {
                    throw new IOException( "Should not have conflicting entries during recovery.", e );
                }
            }
        }
        finally
        {
            searcherManager.release( searcher );
        }
    }

    private void add( long nodeId, Object value, Map<Object, Long> valueToNodeIdInCurrentTx )
            throws IOException, IndexEntryConflictException
    {
        Long previousEntry = valueToNodeIdInCurrentTx.get( value );
        if ( previousEntry == null )
        {
            IndexSearcher searcher = searcherManager.acquire();
            try
            {
                TopDocs docs = searcher.search( documentStructure.newQuery( value ), 1 );
                if ( docs.scoreDocs.length > 0 )
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
                throw new IndexEntryConflictException( nodeId, value, previousEntry );
            }
        }
        else
        {
            valueToNodeIdInCurrentTx.put( value, nodeId );
            writer.addDocument( documentStructure.newDocument( nodeId, value ) );
        }
    }

    private void change( long nodeId, Object valueAfter ) throws IOException
    {
        writer.updateDocument( documentStructure.newQueryForChangeOrRemove( nodeId ),
                               documentStructure.newDocument( nodeId, valueAfter ) );
    }

    private void remove( long nodeId ) throws IOException
    {
        writer.deleteDocuments( documentStructure.newQueryForChangeOrRemove( nodeId ) );
    }
}
