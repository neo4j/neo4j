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
package org.neo4j.internal.counts;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * {@link RelationshipGroupDegreesStore} backed by the {@link GBPTree}.
 * @see GBPTreeGenericCountsStore
 */
class GBPTreeRelationshipGroupDegreesStore extends GBPTreeGenericCountsStore implements RelationshipGroupDegreesStore
{
    private static final String NAME = "Relationship group degrees store";
    static final byte TYPE_DEGREE = (byte) 3;

    GBPTreeRelationshipGroupDegreesStore( PageCache pageCache, Path file, FileSystemAbstraction fileSystem, RecoveryCleanupWorkCollector recoveryCollector,
            DegreesRebuilder rebuilder, boolean readOnly, PageCacheTracer pageCacheTracer, Monitor monitor ) throws IOException
    {
        super( pageCache, file, fileSystem, recoveryCollector, new RebuilderWrapper( rebuilder ), readOnly, NAME, pageCacheTracer, monitor );
    }

    public Updater apply( long txId, PageCursorTracer cursorTracer )
    {
        CountUpdater updater = updater( txId, cursorTracer );
        return updater != null ? new DegreeUpdater( updater ) : NO_OP_UPDATER;
    }

    public long degree( long groupId, RelationshipDirection direction, PageCursorTracer cursorTracer )
    {
        return read( degreeKey( groupId, direction ), cursorTracer );
    }

    private static class DegreeUpdater implements Updater, AutoCloseable
    {
        private final CountUpdater actual;

        DegreeUpdater( CountUpdater actual )
        {
            this.actual = actual;
        }

        @Override
        public void increment( long groupId, RelationshipDirection direction, long delta )
        {
            actual.increment( degreeKey( groupId, direction ), delta );
        }

        @Override
        public void close()
        {
            actual.close();
        }
    }

    static CountsKey degreeKey( long groupId, RelationshipDirection direction )
    {
        return new CountsKey( TYPE_DEGREE, groupId << 2 | direction.ordinal(), 0 );
    }

    public static void dump( PageCache pageCache, Path file, PrintStream out, PageCursorTracer cursorTracer ) throws IOException
    {
        GBPTreeGenericCountsStore.dump( pageCache, file, out, NAME, cursorTracer );
    }

    private static final Updater NO_OP_UPDATER = new Updater()
    {
        @Override
        public void close()
        {
        }

        @Override
        public void increment( long groupId, RelationshipDirection direction, long delta )
        {
        }
    };

    public interface DegreesRebuilder
    {
        void rebuild( Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker );

        long lastCommittedTxId();
    }

    public static final DegreesRebuilder EMPTY_REBUILD = new DegreesRebuilder()
    {
        @Override
        public void rebuild( Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
        }

        @Override
        public long lastCommittedTxId()
        {
            return TransactionIdStore.BASE_TX_ID;
        }
    };

    private static class RebuilderWrapper implements Rebuilder
    {
        private final DegreesRebuilder rebuilder;

        RebuilderWrapper( DegreesRebuilder rebuilder )
        {
            this.rebuilder = rebuilder;
        }

        @Override
        public void rebuild( CountUpdater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            rebuilder.rebuild( new DegreeUpdater( updater ), cursorTracer, memoryTracker );
        }

        @Override
        public long lastCommittedTxId()
        {
            return rebuilder.lastCommittedTxId();
        }
    }
}
