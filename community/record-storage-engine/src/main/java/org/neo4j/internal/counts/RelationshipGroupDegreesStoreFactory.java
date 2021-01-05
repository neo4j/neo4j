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
import java.nio.file.Path;
import java.util.function.LongSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings.FeatureState;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.DegreesRebuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.relaxed_dense_node_locking;

public final class RelationshipGroupDegreesStoreFactory
{
    private RelationshipGroupDegreesStoreFactory()
    {
    }

    public static RelationshipGroupDegreesStore create( Config config, PageCache pageCache, DatabaseLayout layout, FileSystemAbstraction fileSystem,
            RecoveryCleanupWorkCollector recoveryCollector, LongSupplier lastCommitedTxSupplier, PageCacheTracer pageCacheTracer,
            GBPTreeGenericCountsStore.Monitor monitor ) throws IOException
    {
        Path file = layout.relationshipGroupDegreesStore();
        if ( featureEnabled( config, layout, fileSystem ) )
        {
            boolean readOnly = config.get( GraphDatabaseSettings.read_only );
            return new GBPTreeRelationshipGroupDegreesStore( pageCache, file, fileSystem, recoveryCollector, noRebuilder( lastCommitedTxSupplier ),
                    readOnly, pageCacheTracer, monitor );
        }
        if ( fileSystem.fileExists( file ) )
        {
            throw new IllegalStateException( format( "%s is disabled but was previously enabled (%s exists). Can not disable this feature once enabled.",
                    relaxed_dense_node_locking.name(), file.getFileName() ) );
        }
        return NO_GROUP_DEGREES_STORE;
    }

    public static boolean featureEnabled( Config config, DatabaseLayout layout, FileSystemAbstraction fileSystem )
    {
        FeatureState state = config.get( relaxed_dense_node_locking );
        return FeatureState.ENABLED.equals( state ) || FeatureState.AUTO.equals( state ) && fileSystem.fileExists( layout.relationshipGroupDegreesStore() );
    }

    private static DegreesRebuilder noRebuilder( LongSupplier lastCommitedTxSupplier )
    {
        return new DegreesRebuilder()
        {
            @Override
            public long lastCommittedTxId()
            {
                return lastCommitedTxSupplier.getAsLong();
            }

            @Override
            public void rebuild( RelationshipGroupDegreesStore.Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
            {   // TODO We don't support rebuilding this store
            }
        };
    }

    private static final RelationshipGroupDegreesStore NO_GROUP_DEGREES_STORE = new RelationshipGroupDegreesStore()
    {
        @Override
        public Updater apply( long txId, PageCursorTracer cursorTracer )
        {
            return THROWING_UPDATER;
        }

        @Override
        public long degree( long groupId, RelationshipDirection direction, PageCursorTracer cursorTracer )
        {
            throw fail();
        }

        @Override
        public void start( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        { //NOOP
        }

        @Override
        public void close()
        { //NOOP
        }

        @Override
        public void checkpoint( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
        { //NOOP
        }

        private final Updater THROWING_UPDATER = new Updater()
        {
            @Override
            public void close()
            { //NOOP
            }

            @Override
            public void increment( long groupId, RelationshipDirection direction, long delta )
            {
                throw fail();
            }
        };

        private UnsupportedOperationException fail()
        {
            return new UnsupportedOperationException(
                    format( "This store does not support any operations. %s is disabled but the feature was used anyway. Most likely a bug",
                            relaxed_dense_node_locking.name() ) );
        }
    };
}
