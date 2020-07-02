/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.eclipse.collections.api.factory.Sets.immutable;

/**
 * A simple store for keeping index statistics counts, like number of updates, index size, number of unique values a.s.o.
 * These values aren't updated transactionally and so the data is just kept in memory and flushed to a {@link GBPTree} on every checkpoint.
 * Neither reads, writes nor checkpoints block each other.
 *
 * The store is accessible after {@link #init()} has been called.
 */
public class IndexStatisticsStore extends LifecycleAdapter implements IndexStatisticsVisitor.Visitable, ConsistencyCheckable
{
    private static final ImmutableIndexStatistics EMPTY_STATISTICS = new ImmutableIndexStatistics( 0, 0, 0, 0 );

    // Used in GBPTree.seek. Please don't use for writes
    private static final IndexStatisticsKey LOWEST_KEY = new IndexStatisticsKey( Long.MIN_VALUE );
    private static final IndexStatisticsKey HIGHEST_KEY = new IndexStatisticsKey( Long.MAX_VALUE );
    private static final String INIT_TAG = "Initialize IndexStatisticsStore";

    private final PageCache pageCache;
    private final Path path;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final PageCacheTracer pageCacheTracer;
    private final IndexStatisticsLayout layout;
    private final boolean readOnly;
    private GBPTree<IndexStatisticsKey,IndexStatisticsValue> tree;
    // Let IndexStatisticsValue be immutable in this map so that checkpoint doesn't have to coordinate with concurrent writers
    // It's assumed that the data in this map will be so small that everything can just be in it always.
    private final ConcurrentHashMap<Long,ImmutableIndexStatistics> cache = new ConcurrentHashMap<>();

    public IndexStatisticsStore( PageCache pageCache, Path path, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly,
            PageCacheTracer pageCacheTracer )
    {
        this.pageCache = pageCache;
        this.path = path;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.pageCacheTracer = pageCacheTracer;
        this.layout = new IndexStatisticsLayout();
        this.readOnly = readOnly;
    }

    public IndexStatisticsStore( PageCache pageCache, DatabaseLayout databaseLayout, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly, PageCacheTracer pageCacheTracer )
    {
        this( pageCache, databaseLayout.indexStatisticsStore(), recoveryCleanupWorkCollector, readOnly, pageCacheTracer );
    }

    @Override
    public void init() throws IOException
    {
        try
        {
            tree = new GBPTree<>( pageCache, path, layout, GBPTree.NO_MONITOR, GBPTree.NO_HEADER_READER, GBPTree.NO_HEADER_WRITER,
                    recoveryCleanupWorkCollector, readOnly, pageCacheTracer, immutable.empty(), "Statistics store" );
        }
        catch ( TreeFileNotFoundException e )
        {
            throw new IllegalStateException(
                    "Index statistics store file could not be found, most likely this database needs to be recovered, file:" + path, e );
        }
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INIT_TAG ) )
        {
            scanTree( ( key, value ) -> cache.put( key.getIndexId(), new ImmutableIndexStatistics( value ) ), cursorTracer );
        }
    }

    public IndexSample indexSample( long indexId )
    {
        ImmutableIndexStatistics value = cache.getOrDefault( indexId, EMPTY_STATISTICS );
        return new IndexSample( value.indexSize, value.sampleUniqueValues, value.sampleSize, value.updatesCount );
    }

    public void replaceStats( long indexId, IndexSample sample )
    {
        cache.put( indexId, new ImmutableIndexStatistics( sample.uniqueValues(), sample.sampleSize(), sample.updates(), sample.indexSize() ) );
    }

    public void removeIndex( long indexId )
    {
        cache.remove( indexId );
    }

    public void incrementIndexUpdates( long indexId, long delta )
    {
        cache.computeIfPresent( indexId, ( id, existing ) ->
                new ImmutableIndexStatistics( existing.sampleUniqueValues, existing.sampleSize, existing.updatesCount + delta, existing.indexSize ) );
    }

    @Override
    public void visit( IndexStatisticsVisitor visitor, PageCursorTracer cursorTracer )
    {
        try
        {
            scanTree( ( key, value ) -> visitor.visitIndexStatistics( key.getIndexId(),
                    value.getSampleUniqueValues(), value.getSampleSize(), value.getUpdatesCount(), value.getIndexSize() ), cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer ) throws IOException
    {
        if ( !readOnly )
        {
            // There's an assumption that there will never be concurrent calls to checkpoint. This is guarded outside.
            clearTree( cursorTracer );
            writeCacheContentsIntoTree( cursorTracer );
            tree.checkpoint( ioLimiter, cursorTracer );
        }
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return consistencyCheck( reporterFactory.getClass( GBPTreeConsistencyCheckVisitor.class ), cursorTracer );
    }

    private boolean consistencyCheck( GBPTreeConsistencyCheckVisitor<IndexStatisticsKey> visitor, PageCursorTracer cursorTracer )
    {
        try
        {
            return tree.consistencyCheck( visitor, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void scanTree( BiConsumer<IndexStatisticsKey,IndexStatisticsValue> consumer, PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Seeker<IndexStatisticsKey,IndexStatisticsValue> seek = tree.seek( LOWEST_KEY, HIGHEST_KEY, cursorTracer ) )
        {
            while ( seek.next() )
            {
                IndexStatisticsKey key = layout.copyKey( seek.key(), new IndexStatisticsKey() );
                IndexStatisticsValue value = seek.value();
                consumer.accept( key, value );
            }
        }
    }

    private void clearTree( PageCursorTracer cursorTracer ) throws IOException
    {
        // Read all keys from the tree, we can't do this while having a writer since it will grab write lock on pages
        List<IndexStatisticsKey> keys = new ArrayList<>( cache.size() );
        scanTree( ( key, value ) -> keys.add( key ), cursorTracer );

        // Remove all those read keys
        try ( Writer<IndexStatisticsKey,IndexStatisticsValue> writer = tree.writer( cursorTracer ) )
        {
            for ( IndexStatisticsKey key : keys )
            {
                // Idempotent operation
                writer.remove( key );
            }
        }
    }

    private void writeCacheContentsIntoTree( PageCursorTracer cursorTracer ) throws IOException
    {
        try ( Writer<IndexStatisticsKey,IndexStatisticsValue> writer = tree.writer( cursorTracer ) )
        {
            for ( Map.Entry<Long,ImmutableIndexStatistics> entry : cache.entrySet() )
            {
                ImmutableIndexStatistics stats = entry.getValue();
                writer.put( new IndexStatisticsKey( entry.getKey() ),
                        new IndexStatisticsValue( stats.sampleUniqueValues, stats.sampleSize, stats.updatesCount, stats.indexSize ) );
            }
        }
    }

    public Path storeFile()
    {
        return path;
    }

    @Override
    public void shutdown() throws IOException
    {
        tree.close();
    }

    private static class ImmutableIndexStatistics
    {
        private final long sampleUniqueValues;
        private final long sampleSize;
        private final long updatesCount;
        private final long indexSize;

        ImmutableIndexStatistics( long sampleUniqueValues, long sampleSize, long updatesCount, long indexSize )
        {
            this.sampleUniqueValues = sampleUniqueValues;
            this.sampleSize = sampleSize;
            this.updatesCount = updatesCount;
            this.indexSize = indexSize;
        }

        ImmutableIndexStatistics( IndexStatisticsValue value )
        {
            this( value.getSampleUniqueValues(), value.getSampleSize(), value.getUpdatesCount(), value.getIndexSize() );
        }
    }
}
