/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.TaskControl;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.kernel.api.impl.index.DirectorySupport.deleteDirectoryContents;

abstract class LuceneIndexAccessor implements IndexAccessor
{
    protected final LuceneDocumentStructure documentStructure;
    protected final LuceneReferenceManager<IndexSearcher> searcherManager;
    private final boolean readOnly;
    protected final ReservingLuceneIndexWriter writer;

    private final Directory dir;
    private final File indexFolder;
    private final int bufferSizeLimit;
    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    private final PrimitiveLongVisitor<IOException> removeFromLucene = new PrimitiveLongVisitor<IOException>()
    {
        @Override
        public boolean visited( long nodeId ) throws IOException
        {
            LuceneIndexAccessor.this.remove( nodeId );
            return false;
        }
    };

    // we need this wrapping in order to test the index accessor since the ReferenceManager is not mock friendly
    public interface LuceneReferenceManager<G> extends Closeable
    {
        G acquire();

        boolean maybeRefresh() throws IOException;

        void release( G reference ) throws IOException;

        class Wrap<G> implements LuceneReferenceManager<G>
        {
            private final ReferenceManager<G> delegate;

            Wrap( ReferenceManager<G> delegate )
            {

                this.delegate = delegate;
            }

            @Override
            public G acquire()
            {
                return delegate.acquire();
            }

            @Override
            public boolean maybeRefresh() throws IOException
            {
                return delegate.maybeRefresh();
            }

            @Override
            public void release( G reference ) throws IOException
            {
                delegate.release( reference );
            }

            @Override
            public void close() throws IOException
            {
                delegate.close();
            }
        }
    }

    LuceneIndexAccessor( LuceneDocumentStructure documentStructure,
            boolean readOnly,
            IndexWriterFactory<ReservingLuceneIndexWriter> indexWriterFactory,
            DirectoryFactory dirFactory, File indexFolder,
            int bufferSizeLimit ) throws IOException
    {
        this.documentStructure = documentStructure;
        this.readOnly = readOnly;
        this.indexFolder = indexFolder;
        this.bufferSizeLimit = bufferSizeLimit;
        this.dir = dirFactory.open( indexFolder );
        if ( readOnly )
        {
            this.writer = null;
            searcherManager = createReadOnlySearchManager();
        }
        else
        {
            this.writer = indexWriterFactory.create( dir );
            this.searcherManager = new LuceneReferenceManager.Wrap<>( writer.createSearcherManager() );
        }
    }

    private LuceneReferenceManager.Wrap<IndexSearcher> createReadOnlySearchManager() throws IOException
    {
        try
        {
            return new LuceneReferenceManager.Wrap<>( new SearcherManager( dir, new SearcherFactory() ) );
        }
        catch ( IndexNotFoundException e )
        {
            throw new IllegalStateException( "Index creation is not supported in read only mode.", e );
        }
    }

    // test only
    LuceneIndexAccessor( LuceneDocumentStructure documentStructure, boolean readOnly, ReservingLuceneIndexWriter writer,
            LuceneReferenceManager<IndexSearcher> searcherManager,
            Directory dir, File indexFolder, int bufferSizeLimit )
    {
        this.documentStructure = documentStructure;
        this.readOnly = readOnly;
        this.writer = writer;
        this.searcherManager = searcherManager;
        this.dir = dir;
        this.indexFolder = indexFolder;
        this.bufferSizeLimit = bufferSizeLimit;
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Index update is unsupported in read only mode." );
        }
        switch ( mode )
        {
        case ONLINE:
            return new LuceneIndexUpdater( false );

        case BATCHED:
            return new LuceneIndexUpdater( true );

        default:
            throw new ThisShouldNotHappenError( "Stefan", "Unsupported update mode" );
        }
    }

    @Override
    public void drop() throws IOException
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Index drop is unsupported in read only mode." );
        }
        taskCoordinator.cancel();
        closeIndexResources();
        try
        {
            taskCoordinator.awaitCompletion();
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Interrupted while waiting for concurrent tasks to complete.", e );
        }
        deleteDirectoryContents( dir );
    }

    @Override
    public void force() throws IOException
    {
        if ( readOnly )
        {
            return;
        }
        writer.commitAsOnline();
        refreshSearcherManager();
    }

    @Override
    public void flush() throws IOException
    {
        if ( readOnly )
        {
            return;
        }
        refreshSearcherManager();
    }

    @Override
    public void close() throws IOException
    {
        closeIndexResources();
        dir.close();
    }

    @Override
    public IndexReader newReader()
    {
        final IndexSearcher searcher = searcherManager.acquire();
        final TaskControl token = taskCoordinator.newInstance();
        final Closeable closeable = new Closeable()
        {
            @Override
            public void close() throws IOException
            {
                searcherManager.release( searcher );
                token.close();
            }
        };
        return makeNewReader( searcher, closeable, token );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new LuceneAllEntriesIndexAccessorReader( new LuceneAllDocumentsReader( searcherManager ),
                documentStructure );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        LuceneSnapshotter snapshotter = new LuceneSnapshotter();
        return readOnly ? snapshotter.snapshot( indexFolder, dir ) : snapshotter.snapshot( this.indexFolder, writer );
    }

    protected IndexReader makeNewReader( IndexSearcher searcher, Closeable closeable, CancellationRequest cancellation )
    {
        return new LuceneIndexAccessorReader( searcher, documentStructure, closeable, cancellation, bufferSizeLimit );
    }

    private void closeIndexResources() throws IOException
    {
        if ( writer != null )
        {
            writer.close();
        }
        searcherManager.close();
    }

    private void addRecovered( long nodeId, Object value ) throws IOException, IndexCapacityExceededException
    {
        Fieldable encodedValue = documentStructure.encodeAsFieldable( value );
        writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                documentStructure.newDocumentRepresentingProperty( nodeId, encodedValue ) );
    }

    protected void add( long nodeId, Object value ) throws IOException, IndexCapacityExceededException
    {
        Fieldable encodedValue = documentStructure.encodeAsFieldable( value );
        writer.addDocument( documentStructure.newDocumentRepresentingProperty( nodeId, encodedValue ) );
    }

    protected void change( long nodeId, Object value ) throws IOException, IndexCapacityExceededException
    {
        Fieldable encodedValue = documentStructure.encodeAsFieldable( value );
        writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                documentStructure.newDocumentRepresentingProperty( nodeId, encodedValue ) );
    }

    protected void remove( long nodeId ) throws IOException
    {
        writer.deleteDocuments( documentStructure.newTermForChangeOrRemove( nodeId ) );
    }

    // This method should be synchronized because we need every thread to perform actual refresh
    // and not just skip it because some other refresh is in progress
    private synchronized void refreshSearcherManager() throws IOException
    {
        searcherManager.maybeRefresh();
    }

    private class LuceneIndexUpdater implements IndexUpdater
    {
        private final boolean isBatched;

        private LuceneIndexUpdater( boolean inBatched )
        {
            this.isBatched = inBatched;
        }

        @Override
        public Reservation validate( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexCapacityExceededException
        {
            int insertionsCount = 0;
            for ( NodePropertyUpdate update : updates )
            {
                // Only count additions and updates, since removals will not affect the size of the index
                // until it is merged. Each update is in fact atomic (delete + add).
                if ( update.getUpdateMode() == UpdateMode.ADDED || update.getUpdateMode() == UpdateMode.CHANGED )
                {
                    insertionsCount++;
                }
            }

            writer.reserveInsertions( insertionsCount );

            final int insertions = insertionsCount;
            return new Reservation()
            {
                boolean released;

                @Override
                public void release()
                {
                    if ( released )
                    {
                        throw new IllegalStateException( "Reservation was already released. " +
                                                         "Previously reserved " + insertions + " insertions" );
                    }
                    writer.removeReservedInsertions( insertions );
                    released = true;
                }
            };
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException, IndexCapacityExceededException
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                if ( isBatched )
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
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            if ( !isBatched )
            {
                refreshSearcherManager();
            }
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds ) throws IOException
        {
            nodeIds.visitKeys( removeFromLucene );
        }
    }
}

