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
package org.neo4j.kernel.impl.api.index.stats;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeConsistencyCheckVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.api.index.stats.IndexStatisticsValue.EMPTY_STATISTICS;

/**
 * A simple store for keeping index statistics counts, like number of updates, index size, number of unique values a.s.o.
 * These values aren't updated transactionally and so the data is just kept in memory and flushed to a {@link GBPTree} on every checkpoint.
 * Neither reads, writes nor checkpoints block each other.
 *
 * The store is accessible after {@link #init()} has been called.
 */
public class IndexStatisticsStore extends LifecycleAdapter implements IndexStatisticsVisitor.Visitable, ConsistencyCheckable
{
    // Used in GBPTree.seek. Please don't use for writes
    private static final IndexStatisticsKey LOWEST_KEY = new IndexStatisticsKey( Long.MIN_VALUE );
    private static final IndexStatisticsKey HIGHEST_KEY = new IndexStatisticsKey( Long.MAX_VALUE );

    private final PageCache pageCache;
    private final File file;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final IndexStatisticsLayout layout;
    private final boolean readOnly;
    private GBPTree<IndexStatisticsKey,IndexStatisticsValue> tree;
    // Let IndexStatisticsValue be immutable in this map so that checkpoint doesn't have to coordinate with concurrent writers
    // It's assumed that the data in this map will be so small that everything can just be in it always.
    private final ConcurrentHashMap<IndexStatisticsKey,IndexStatisticsValue> cache = new ConcurrentHashMap<>();

    public IndexStatisticsStore( PageCache pageCache, File file, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.file = file;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.layout = new IndexStatisticsLayout();
        this.readOnly = readOnly;
    }

    public IndexStatisticsStore( PageCache pageCache, DatabaseLayout databaseLayout, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly )
    {
        this( pageCache, databaseLayout.indexStatisticsStore(), recoveryCleanupWorkCollector, readOnly );
    }

    @Override
    public void init() throws IOException
    {
        try
        {
            tree = new GBPTree<>( pageCache, file, layout, 0, GBPTree.NO_MONITOR, GBPTree.NO_HEADER_READER, GBPTree.NO_HEADER_WRITER,
                    recoveryCleanupWorkCollector, readOnly, PageCacheTracer.NULL );
        }
        catch ( TreeFileNotFoundException e )
        {
            throw new IllegalStateException(
                    "Index statistics store file could not be found, most likely this database needs to be recovered, file:" + file, e );
        }
        scanTree( cache::put );
    }

    public IndexSample indexSample( long indexId )
    {
        IndexStatisticsValue value = cache.getOrDefault( new IndexStatisticsKey( indexId ), EMPTY_STATISTICS );
        return new IndexSample( value.getIndexSize(), value.getSampleUniqueValues(), value.getSampleSize(), value.getUpdatesCount() );
    }

    public void replaceStats( long indexId, IndexSample sample )
    {
        assertNotReadOnly();
        IndexStatisticsKey key = new IndexStatisticsKey( indexId );
        IndexStatisticsValue value = new IndexStatisticsValue( sample.uniqueValues(), sample.sampleSize(), sample.updates(), sample.indexSize() );
        cache.put( key, value );
    }

    public void removeIndex( long indexId )
    {
        assertNotReadOnly();
        cache.remove( new IndexStatisticsKey( indexId ) );
    }

    public void incrementIndexUpdates( long indexId, long delta )
    {
        assertNotReadOnly();
        IndexStatisticsKey key = new IndexStatisticsKey( indexId );
        boolean replaced;
        do
        {
            IndexStatisticsValue existing = cache.get( key );
            if ( existing == null )
            {
                return;
            }
            IndexStatisticsValue value = new IndexStatisticsValue(
                    existing.getSampleUniqueValues(), existing.getSampleSize(), existing.getUpdatesCount() + delta, existing.getIndexSize() );
            replaced = cache.replace( key, existing, value );
        }
        while ( !replaced );
    }

    @Override
    public void visit( IndexStatisticsVisitor visitor )
    {
        try
        {
            scanTree( ( key, value ) -> visitor.visitIndexStatistics( key.getIndexId(),
                    value.getSampleUniqueValues(), value.getSampleSize(), value.getUpdatesCount(), value.getIndexSize() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public void checkpoint( IOLimiter ioLimiter ) throws IOException
    {
        if ( !readOnly )
        {
            // There's an assumption that there will never be concurrent calls to checkpoint. This is guarded outside.
            clearTree();
            writeCacheContentsIntoTree();
            tree.checkpoint( ioLimiter, TRACER_SUPPLIER.get() );
        }
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ) );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<IndexStatisticsKey> visitor )
    {
        try
        {
            return tree.consistencyCheck( visitor, TRACER_SUPPLIER.get() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void scanTree( BiConsumer<IndexStatisticsKey,IndexStatisticsValue> consumer ) throws IOException
    {
        try ( Seeker<IndexStatisticsKey,IndexStatisticsValue> seek = tree.seek( LOWEST_KEY, HIGHEST_KEY, TRACER_SUPPLIER.get() ) )
        {
            while ( seek.next() )
            {
                IndexStatisticsKey key = layout.copyKey( seek.key(), new IndexStatisticsKey() );
                IndexStatisticsValue value = seek.value().copy();
                consumer.accept( key, value );
            }
        }
    }

    private void clearTree() throws IOException
    {
        // Read all keys from the tree, we can't do this while having a writer since it will grab write lock on pages
        List<IndexStatisticsKey> keys = new ArrayList<>( cache.size() );
        scanTree( ( key, value ) -> keys.add( key ) );

        // Remove all those read keys
        try ( Writer<IndexStatisticsKey,IndexStatisticsValue> writer = tree.writer( TRACER_SUPPLIER.get() ) )
        {
            for ( IndexStatisticsKey key : keys )
            {
                // Idempotent operation
                writer.remove( key );
            }
        }
    }

    private void writeCacheContentsIntoTree() throws IOException
    {
        try ( Writer<IndexStatisticsKey,IndexStatisticsValue> writer = tree.writer( TRACER_SUPPLIER.get() ) )
        {
            for ( Map.Entry<IndexStatisticsKey,IndexStatisticsValue> entry : cache.entrySet() )
            {
                writer.put( entry.getKey(), entry.getValue() );
            }
        }
    }

    private void assertNotReadOnly()
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can not write to index statistics store while in read only mode." );
        }
    }

    public File storeFile()
    {
        return file;
    }

    @Override
    public void shutdown() throws IOException
    {
        tree.close();
    }
}
