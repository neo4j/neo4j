/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;

public class LuceneIndexAccessor implements IndexAccessor
{
    private final LuceneIndexWriter writer;
    private SchemaIndex luceneIndex;

    public LuceneIndexAccessor( SchemaIndex luceneIndex ) throws IOException
    {
        this.luceneIndex = luceneIndex;
        this.writer = luceneIndex.isReadOnly() ? null : luceneIndex.getIndexWriter();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create updated for read only index." );
        }
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
            return luceneIndex.getIndexReader();
        }
        catch ( IOException e )
        {
            throw new LuceneIndexReaderAcquisitionException( "Can't acquire index reader", e );
        }
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new LuceneAllEntriesIndexAccessorReader( luceneIndex.allDocumentsReader() );
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
        public void process( IndexEntryUpdate update ) throws IOException
        {
            switch ( update.updateMode() )
            {
            case ADDED:
                if ( isRecovery )
                {
                    addRecovered( update.getEntityId(), update.values()[0] );
                }
                else
                {
                    add( update.getEntityId(), update.values()[0]  );
                }
                break;
            case CHANGED:
                change( update.getEntityId(), update.values()[0]  );
                break;
            case REMOVED:
                remove( update.getEntityId() );
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

            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        private void add( long nodeId, Object value ) throws IOException
        {
            writer.addDocument( LuceneDocumentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        private void change( long nodeId, Object value ) throws IOException
        {
            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperty( nodeId, value ) );
        }

        protected void remove( long nodeId ) throws IOException
        {
            writer.deleteDocuments( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        }
    }
}

