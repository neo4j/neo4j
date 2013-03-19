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

import static org.neo4j.kernel.api.impl.index.DirectorySupport.deleteDirectoryContents;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.DocumentLogic;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.WriterLogic;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

class LuceneIndexAccessor implements IndexAccessor
{
    private final IndexWriter writer;
    private final Directory dir;
    private final DocumentLogic documentLogic;
    private final WriterLogic writerLogic;
    private final SearcherManager searcherManager;

    public LuceneIndexAccessor( LuceneIndexWriterFactory indexWriterFactory, DirectoryFactory dirFactory, File dirFile,
            DocumentLogic documentLogic, WriterLogic writerLogic ) throws IOException
    {
        this.dir = dirFactory.open( dirFile );
        this.documentLogic = documentLogic;
        this.writerLogic = writerLogic;
        this.writer = indexWriterFactory.create( dir );
        this.searcherManager = new SearcherManager( writer, true, new SearcherFactory());
    }

    @Override
    public void drop() throws IOException
    {
        writerLogic.close( writer );
        deleteDirectoryContents( dir );
    }

    @Override
    public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                add( update.getNodeId(), update.getValueAfter() );
                break;
            case CHANGED:
                change( update.getNodeId(), update.getValueBefore(), update.getValueAfter() );
                break;
            case REMOVED:
                remove( update.getNodeId(), update.getValueBefore() );
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
                change( update.getNodeId(), update.getValueBefore(), update.getValueAfter() );
                break;
            case REMOVED:
                remove( update.getNodeId(), update.getValueBefore() );
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
        TopDocs hits = searcher.search( new TermQuery( documentLogic.newQueryForChangeOrRemove( nodeId ) ), 1 );
        if ( hits.totalHits > 0 )
        {
            writer.updateDocument( documentLogic.newQueryForChangeOrRemove( nodeId ), documentLogic.newDocument( nodeId, value ) );
        }
        else
        {
            add( nodeId, value );
        }
    }

    private void add( long nodeId, Object value ) throws IOException
    {
        writer.addDocument( documentLogic.newDocument( nodeId, value ) );
    }

    private void change( long nodeId, Object valueBefore, Object valueAfter ) throws IOException
    {
        writer.updateDocument( documentLogic.newQueryForChangeOrRemove( nodeId ),
                documentLogic.newDocument( nodeId, valueAfter ) );
    }
    
    private void remove( long nodeId, Object value ) throws IOException
    {
        writer.deleteDocuments( documentLogic.newQueryForChangeOrRemove( nodeId ) );
    }
    
    @Override
    public void force() throws IOException
    {
        writerLogic.forceAndMarkAsOnline( writer );
    }

    @Override
    public void close() throws IOException
    {
        writerLogic.close( writer );
        dir.close();
    }

    @Override
    public IndexReader newReader()
    {
        return new LuceneIndexAccessorReader( searcherManager, documentLogic );
    }
}
