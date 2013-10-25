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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static org.neo4j.kernel.api.impl.index.DirectorySupport.deleteDirectoryContents;

abstract class LuceneIndexAccessor implements IndexAccessor
{
    protected final LuceneDocumentStructure documentStructure;
    protected final SearcherManager searcherManager;
    protected final IndexWriter writer;
    private final IndexWriterStatus writerStatus;
    private final Directory dir;
    private final File dirFile;

    LuceneIndexAccessor( LuceneDocumentStructure documentStructure, LuceneIndexWriterFactory indexWriterFactory,
                         IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile )
            throws IOException
    {
        this.documentStructure = documentStructure;
        this.dirFile = dirFile;
        this.dir = dirFactory.open( dirFile );
        this.writer = indexWriterFactory.create( dir );
        this.writerStatus = writerStatus;
        this.searcherManager = new SearcherManager( writer, true, new SearcherFactory() );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode ) throws IOException
    {
        switch (mode)
        {
            case ONLINE:
                return new LuceneIndexUpdater( false );

            case RECOVERY:
                return new LuceneIndexUpdater( true );

            default:
                throw new ThisShouldNotHappenError( "Stefan", "Unsupported update mode" );
        }
    };

    @Override
    public void drop() throws IOException
    {
        closeIndexResources();
        deleteDirectoryContents( dir );
    }

    @Override
    public void force() throws IOException
    {
        writerStatus.commitAsOnline( writer );
    }

    @Override
    public void close() throws IOException
    {
        closeIndexResources();
        dir.close();
    }

    private void closeIndexResources() throws IOException
    {
        writerStatus.close( writer );
        searcherManager.close();
    }

    @Override
    public IndexReader newReader()
    {
        return new LuceneIndexAccessorReader( searcherManager, documentStructure );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        SnapshotDeletionPolicy deletionPolicy = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        return new LuceneStoreSnapshot( dirFile, deletionPolicy );
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
                        documentStructure.newDocumentRepresentingProperty( nodeId, value ) );
            }
            else
            {
                add( nodeId, value );
            }
        }
        finally
        {
            searcherManager.release( searcher );
        }
    }

    protected void add( long nodeId, Object value ) throws IOException
    {
        writer.addDocument( documentStructure.newDocumentRepresentingProperty( nodeId, value ) );
    }

    protected void change( long nodeId, Object valueAfter ) throws IOException
    {
        writer.updateDocument( documentStructure.newQueryForChangeOrRemove( nodeId ),
                documentStructure.newDocumentRepresentingProperty( nodeId, valueAfter ) );
    }

    protected void remove( long nodeId ) throws IOException
    {
        writer.deleteDocuments( documentStructure.newQueryForChangeOrRemove( nodeId ) );
    }

    private class LuceneIndexUpdater implements IndexUpdater
    {
        private final boolean inRecovery;

        private LuceneIndexUpdater( boolean inRecovery )
        {
            this.inRecovery = inRecovery;
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException
        {
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    if ( inRecovery )
                    {
                        addRecovered( update.getNodeId(), update.getValueAfter() );
                    }
                    else
                    {
                        add( update.getNodeId(), update.getValueAfter() );
                    }
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

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            searcherManager.maybeRefresh();
        }
    }
}

