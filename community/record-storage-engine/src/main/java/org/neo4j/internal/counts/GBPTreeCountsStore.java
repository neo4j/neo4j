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
package org.neo4j.internal.counts;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;

/**
 * Counts store build on top of the {@link GBPTree}.
 * Changes between checkpoints are kept in memory and written out to the tree in {@link #checkpoint(IOLimiter, PageCursorTracer)}.
 * Multiple {@link #apply(long, PageCursorTracer)} appliers} can run concurrently in a lock-free manner.
 * Checkpoint will acquire a write lock, wait for currently active appliers to close while at the same time blocking new appliers to start,
 * but doesn't wait for appliers that haven't even started yet, i.e. it doesn't require a gap-free transaction sequence to be completed.
 */
public class GBPTreeCountsStore extends GBPTreeGenericCountsStore implements CountsStore
{
    public GBPTreeCountsStore( PageCache pageCache, Path file, FileSystemAbstraction fileSystem, RecoveryCleanupWorkCollector recoveryCollector,
            CountsBuilder initialCountsBuilder, boolean readOnly, PageCacheTracer pageCacheTracer, Monitor monitor ) throws IOException
    {
        super( pageCache, file, fileSystem, recoveryCollector, new InitialCountsRebuilder( initialCountsBuilder ), readOnly, pageCacheTracer, monitor );
    }

    @Override
    public CountsAccessor.Updater apply( long txId, PageCursorTracer cursorTracer )
    {
        CountUpdater updater = updater( txId, cursorTracer );
        return updater != null ? new Incrementer( updater ) : NO_OP_UPDATER;
    }

    @Override
    public long nodeCount( int labelId, PageCursorTracer cursorTracer )
    {
        return read( nodeKey( labelId ), cursorTracer );
    }

    @Override
    public long relationshipCount( int startLabelId, int typeId, int endLabelId, PageCursorTracer cursorTracer )
    {
        return read( relationshipKey( startLabelId, typeId, endLabelId ), cursorTracer );
    }

    @Override
    public void accept( CountsVisitor visitor, PageCursorTracer cursorTracer )
    {
        // First visit the changes that we haven't check-pointed yet
        for ( Map.Entry<CountsKey,AtomicLong> changedEntry : changes.sortedChanges( layout ) )
        {
            // Our simplistic approach to the changes map makes it contain 0 counts at times, we don't remove entries from it
            if ( changedEntry.getValue().get() != 0 )
            {
                changedEntry.getKey().accept( visitor, changedEntry.getValue().get() );
            }
        }

        // Then visit the remaining stored changes from the last check-point
        try ( Seeker<CountsKey,CountsValue> seek = tree.seek( CountsKey.MIN_COUNT, CountsKey.MAX_COUNT, cursorTracer ) )
        {
            while ( seek.next() )
            {
                CountsKey key = seek.key();
                if ( !changes.containsChange( key ) )
                {
                    key.accept( visitor, seek.value().count );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private static class Incrementer implements CountsAccessor.Updater
    {
        private final CountUpdater actual;

        Incrementer( CountUpdater actual )
        {
            this.actual = actual;
        }

        @Override
        public void incrementNodeCount( long labelId, long delta )
        {
            actual.increment( nodeKey( labelId ), delta );
        }

        @Override
        public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
        {
            actual.increment( relationshipKey( startLabelId, typeId, endLabelId ), delta );
        }

        @Override
        public void close()
        {
            actual.close();
        }
    }

    private static class InitialCountsRebuilder implements Rebuilder
    {
        private final CountsBuilder initialCountsBuilder;

        InitialCountsRebuilder( CountsBuilder initialCountsBuilder )
        {
            this.initialCountsBuilder = initialCountsBuilder;
        }

        @Override
        public long lastCommittedTxId()
        {
            return initialCountsBuilder.lastCommittedTxId();
        }

        @Override
        public void rebuild( CountUpdater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            initialCountsBuilder.initialize( new Incrementer( updater ), cursorTracer, memoryTracker );
        }
    }
}
