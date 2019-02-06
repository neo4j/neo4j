/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.ToLongFunction;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneIndexReaderAcquisitionException;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

public abstract class AbstractLuceneIndexAccessor<READER extends IndexReader, INDEX extends DatabaseIndex<READER>> implements IndexAccessor
{
    protected final LuceneIndexWriter writer;
    protected final INDEX luceneIndex;
    protected final IndexDescriptor descriptor;

    protected AbstractLuceneIndexAccessor( INDEX luceneIndex, IndexDescriptor descriptor )
    {
        this.writer = luceneIndex.isReadOnly() ? null : luceneIndex.getIndexWriter();
        this.luceneIndex = luceneIndex;
        this.descriptor = descriptor;
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create updater for read only index." );
        }
        return getIndexUpdater( mode );
    }

    protected abstract IndexUpdater getIndexUpdater( IndexUpdateMode mode );

    @Override
    public void drop()
    {
        luceneIndex.drop();
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        try
        {
            // We never change status of read-only indexes.
            if ( !luceneIndex.isReadOnly() )
            {
                luceneIndex.markAsOnline();
            }
            luceneIndex.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void refresh()
    {
        try
        {
            luceneIndex.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            luceneIndex.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public READER newReader()
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

    public BoundedIterable<Long> newAllEntriesReader( ToLongFunction<Document> entityIdReader )
    {
        return new LuceneAllEntriesIndexAccessorReader( luceneIndex.allDocumentsReader(), entityIdReader );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        try
        {
            return luceneIndex.snapshot();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public abstract void verifyDeferredConstraints( NodePropertyAccessor propertyAccessor ) throws IndexEntryConflictException;

    @Override
    public boolean isDirty()
    {
        return !luceneIndex.isValid();
    }

    protected abstract class AbstractLuceneIndexUpdater implements IndexUpdater
    {
        private final boolean idempotent;
        private final boolean refresh;

        private boolean hasChanges;

        protected AbstractLuceneIndexUpdater( boolean idempotent, boolean refresh )
        {
            this.idempotent = idempotent;
            this.refresh = refresh;
        }

        @Override
        public void process( IndexEntryUpdate<?> update )
        {
            // we do not support adding partial entries
            assert update.indexKey().schema().equals( descriptor.schema() );

            switch ( update.updateMode() )
            {
            case ADDED:
                if ( idempotent )
                {
                    addIdempotent( update.getEntityId(), update.values() );
                }
                else
                {
                    add( update.getEntityId(), update.values() );
                }
                break;
            case CHANGED:
                change( update.getEntityId(), update.values() );
                break;
            case REMOVED:
                remove( update.getEntityId() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
            hasChanges = true;
        }

        @Override
        public void close()
        {
            if ( hasChanges && refresh )
            {
                try
                {
                    luceneIndex.maybeRefreshBlocking();
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        }

        protected abstract void addIdempotent( long nodeId, Value[] values );

        protected abstract void add( long nodeId, Value[] values );

        protected abstract void change( long nodeId, Value[] values );

        protected abstract void remove( long nodeId );
    }
}
