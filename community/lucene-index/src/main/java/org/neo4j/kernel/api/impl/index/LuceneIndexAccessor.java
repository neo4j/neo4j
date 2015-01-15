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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.kernel.api.impl.index.DirectorySupport.deleteDirectoryContents;

abstract class LuceneIndexAccessor implements IndexAccessor
{
    protected final LuceneDocumentStructure documentStructure;
    protected final SearcherManager searcherManager;
    protected final LuceneIndexWriter writer;

    private final Directory dir;
    private final File dirFile;

    LuceneIndexAccessor( LuceneDocumentStructure documentStructure, LuceneIndexWriterFactory indexWriterFactory,
                         DirectoryFactory dirFactory, File dirFile )
            throws IOException
    {
        this.documentStructure = documentStructure;
        this.dirFile = dirFile;
        this.dir = dirFactory.open( dirFile );
        this.writer = indexWriterFactory.create( dir );
        this.searcherManager = writer.createSearcherManager();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        switch ( mode )
        {
            case ONLINE:
                return new LuceneIndexUpdater( false );

            case RECOVERY:
                return new LuceneIndexUpdater( true );

            default:
                throw new ThisShouldNotHappenError( "Stefan", "Unsupported update mode" );
        }
    }

    @Override
    public void drop() throws IOException
    {
        closeIndexResources();
        deleteDirectoryContents( dir );
    }

    @Override
    public void force() throws IOException
    {
        writer.commitAsOnline();
    }

    @Override
    public void close() throws IOException
    {
        closeIndexResources();
        dir.close();
    }

    private void closeIndexResources() throws IOException
    {
        writer.close();
        searcherManager.close();
    }

    @Override
    public IndexReader newReader()
    {
        return new LuceneIndexAccessorReader( searcherManager, documentStructure );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new LuceneAllEntriesIndexAccessorReader( new LuceneAllDocumentsReader( searcherManager ), documentStructure );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return new LuceneSnapshotter().snapshot( this.dirFile, writer );
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

    synchronized void refreshSearcherManager() throws IOException
    {
        searcherManager.maybeRefresh();
    }

    private class LuceneIndexUpdater implements IndexUpdater
    {
        private final boolean inRecovery;

        private LuceneIndexUpdater( boolean inRecovery )
        {
            this.inRecovery = inRecovery;
        }

        @Override
        public PreparedIndexUpdates prepare( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
            int insertions = 0;
            for ( NodePropertyUpdate update : updates )
            {
                // Only count additions and updates, since removals will not affect the size of the index until it is
                // merged or optimized
                if ( update.getUpdateMode() == UpdateMode.ADDED || update.getUpdateMode() == UpdateMode.CHANGED )
                {
                    insertions++;
                }
            }

            writer.reserveDocumentInsertions( insertions );

            return new LucenePreparedIndexUpdates( updates, insertions, inRecovery );
        }

        @Override
        public void remove( Iterable<Long> nodeIds ) throws IOException
        {
            for ( long nodeId : nodeIds )
            {
                LuceneIndexAccessor.this.remove( nodeId );
            }
        }
    }

    private class LucenePreparedIndexUpdates implements PreparedIndexUpdates
    {
        final Iterable<NodePropertyUpdate> updates;
        final int insertions;
        final boolean inRecovery;

        boolean committed;
        boolean rolledBack;

        LucenePreparedIndexUpdates( Iterable<NodePropertyUpdate> updates, int insertions, boolean inRecovery )
        {
            this.updates = updates;
            this.insertions = insertions;
            this.inRecovery = inRecovery;
        }

        @Override
        public void commit() throws IOException
        {
            checkStatus();
            try
            {
                committed = true;
                for ( NodePropertyUpdate update : updates )
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
                        LuceneIndexAccessor.this.remove( update.getNodeId() );
                        break;
                    default:
                        throw new UnsupportedOperationException( "Unknown update mode: " + update.getUpdateMode() );
                    }
                }

                refreshSearcherManager(); // to allow all following reads see committed changes
            }
            finally
            {
                removeReservation();
            }
        }

        @Override
        public void rollback()
        {
            checkStatus();
            rolledBack = true;
            removeReservation();
        }

        void checkStatus()
        {
            if ( committed )
            {
                throw new IllegalStateException( "Index updates were already committed" );
            }
            if ( rolledBack )
            {
                throw new IllegalStateException( "Index updates were already rolled back" );
            }
        }

        void removeReservation()
        {
            writer.removeReservationOfDocumentInsertions( insertions );
        }
    }
}

