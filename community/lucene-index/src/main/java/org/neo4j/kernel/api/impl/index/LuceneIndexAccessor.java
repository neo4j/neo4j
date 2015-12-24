/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;


abstract class LuceneIndexAccessor implements IndexAccessor
{
    protected final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
    private final LuceneIndexWriter writer;
    private LuceneIndex luceneIndex;

    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    LuceneIndexAccessor( LuceneIndex luceneIndex ) throws IOException
    {
        this.luceneIndex = luceneIndex;
        this.writer = luceneIndex.getIndexWriter();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        switch ( mode )
        {
        case ONLINE:
            return new LuceneIndexUpdater( writer, false );

        case RECOVERY:
            return new LuceneIndexUpdater( writer, true );

        default:
            throw new IllegalArgumentException( "Unsupported update mode: " + mode );
        }
    }

    @Override
    public void drop() throws IOException
    {
        taskCoordinator.cancel();
        try
        {
            taskCoordinator.awaitCompletion();
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Interrupted while waiting for concurrent tasks to complete.", e );
        }
        luceneIndex.drop();
    }

    @Override
    public void force() throws IOException
    {
        luceneIndex.markAsOnline();
        luceneIndex.maybeRefreshBlocking();
    }

    @Override
    public void flush() throws IOException
    {
        luceneIndex.maybeRefreshBlocking();
    }

    @Override
    public void close() throws IOException
    {
        luceneIndex.close();
    }

    @Override
    public IndexReader newReader()
    {
        try
        {
            // TODO: task control stuff
            return luceneIndex.getIndexReader();
        }
        catch ( IOException e )
        {
            throw new LuceneIndexAcquisitionException("Can't acquire index reader");
        }
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        try
        {
            LuceneAllDocumentsReader allDocumentsReader = new LuceneAllDocumentsReader( luceneIndex.getIndexReader() );
            return new LuceneAllEntriesIndexAccessorReader( allDocumentsReader );
        }
        catch ( Exception e )
        {
            // TODO:
            throw new RuntimeException( e );
        }
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return luceneIndex.snapshot();
    }

    private class LuceneIndexUpdater implements IndexUpdater
    {
        private final boolean isRecovery;
        private final LuceneIndexWriter writer;

        private LuceneIndexUpdater( LuceneIndexWriter indexWriter, boolean isRecovery )
        {
            this.isRecovery = isRecovery;
            this.writer = indexWriter;
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                if ( isRecovery )
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
            luceneIndex.maybeRefreshBlocking();
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds ) throws IOException
        {
            nodeIds.visitKeys( nodeId -> {
                remove( nodeId );
                return false;
            } );
        }

        private void addRecovered( long nodeId, Object value ) throws IOException
        {

            writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                    documentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        private void add( long nodeId, Object value ) throws IOException
        {
            writer.addDocument( documentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        private void change( long nodeId, Object value ) throws IOException
        {
            writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                    documentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        protected void remove( long nodeId ) throws IOException
        {
            writer.deleteDocuments( documentStructure.newTermForChangeOrRemove( nodeId ) );
        }
    }
}

