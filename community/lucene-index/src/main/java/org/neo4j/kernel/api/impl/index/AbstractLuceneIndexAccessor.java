/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneIndexReaderAcquisitionException;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

public abstract class AbstractLuceneIndexAccessor<READER extends ValueIndexReader, INDEX extends DatabaseIndex<READER>> implements IndexAccessor
{
    protected final LuceneIndexWriter writer;
    protected final INDEX luceneIndex;
    protected final IndexDescriptor descriptor;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;

    protected AbstractLuceneIndexAccessor( INDEX luceneIndex, IndexDescriptor descriptor, IndexUpdateIgnoreStrategy ignoreStrategy )
    {
        this.writer = luceneIndex.isReadOnly() ? null : luceneIndex.getIndexWriter();
        this.luceneIndex = luceneIndex;
        this.descriptor = descriptor;
        this.ignoreStrategy = ignoreStrategy;
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode, CursorContext cursorContext )
    {
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create index updater while database is in read only mode." );
        }
        return getIndexUpdater( mode );
    }

    protected abstract IndexUpdater getIndexUpdater( IndexUpdateMode mode );

    @Override
    public void drop()
    {
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't drop index while database is in read only mode." );
        }
        luceneIndex.drop();
    }

    @Override
    public void force( CursorContext cursorContext )
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
    public READER newValueReader()
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

    public BoundedIterable<Long> newAllEntriesReader( ToLongFunction<Document> entityIdReader, long fromIdInclusive, long toIdExclusive )
    {
        return new LuceneAllEntriesIndexAccessorReader( luceneIndex.allDocumentsReader(), entityIdReader, fromIdInclusive, toIdExclusive );
    }

    public IndexEntriesReader[] newAllEntriesValueReader( ToLongFunction<Document> entityIdReader, int numPartitions )
    {
        LuceneAllDocumentsReader allDocumentsReader = luceneIndex.allDocumentsReader();
        List<Iterator<Document>> partitions = allDocumentsReader.partition( numPartitions );
        AtomicInteger closeCount = new AtomicInteger( partitions.size() );
        List<IndexEntriesReader> readers = partitions.stream().map( partitionDocuments -> new PartitionIndexEntriesReader( closeCount, allDocumentsReader,
                entityIdReader, partitionDocuments ) ).collect( Collectors.toList() );
        return readers.toArray( IndexEntriesReader[]::new );
    }

    @Override
    public ResourceIterator<Path> snapshotFiles()
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
    public boolean consistencyCheck( ReporterFactory reporterFactory, CursorContext cursorContext )
    {
        final LuceneIndexConsistencyCheckVisitor visitor = reporterFactory.getClass( LuceneIndexConsistencyCheckVisitor.class );
        final boolean isConsistent = luceneIndex.isValid();
        if ( !isConsistent )
        {
            visitor.isInconsistent( descriptor );
        }
        return isConsistent;
    }

    @Override
    public long estimateNumberOfEntries( CursorContext ignored )
    {
        return luceneIndex.allDocumentsReader().maxCount();
    }

    private static class PartitionIndexEntriesReader implements IndexEntriesReader
    {
        private final AtomicInteger closeCount;
        private final LuceneAllDocumentsReader allDocumentsReader;
        private final ToLongFunction<Document> entityIdReader;
        private final Iterator<Document> partitionDocuments;

        PartitionIndexEntriesReader( AtomicInteger closeCount, LuceneAllDocumentsReader allDocumentsReader, ToLongFunction<Document> entityIdReader,
                Iterator<Document> partitionDocuments )
        {
            this.closeCount = closeCount;
            this.allDocumentsReader = allDocumentsReader;
            this.entityIdReader = entityIdReader;
            this.partitionDocuments = partitionDocuments;
        }

        @Override
        public Value[] values()
        {
            return null;
        }

        @Override
        public void close()
        {
            // Since all these (sub-range) readers come from the one LuceneAllDocumentsReader it will have to remain open until the last reader is closed
            if ( closeCount.decrementAndGet() == 0 )
            {
                try
                {
                    allDocumentsReader.close();
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        }

        @Override
        public long next()
        {
            return entityIdReader.applyAsLong( partitionDocuments.next() );
        }

        @Override
        public boolean hasNext()
        {
            return partitionDocuments.hasNext();
        }
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
            assert update.indexKey().schema().equals( descriptor.schema() );
            ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate( update );
            if ( ignoreStrategy.ignore( valueUpdate ) )
            {
                return;
            }

            switch ( valueUpdate.updateMode() )
            {
            case ADDED:
                if ( idempotent )
                {
                    addIdempotent( valueUpdate.getEntityId(), valueUpdate.values() );
                }
                else
                {
                    add( valueUpdate.getEntityId(), valueUpdate.values() );
                }
                break;
            case CHANGED:
                change( valueUpdate.getEntityId(), valueUpdate.values() );
                break;
            case REMOVED:
                remove( valueUpdate.getEntityId() );
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
