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
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * {@link RelationshipGroupDegreesStore} backed by the {@link GBPTree}.
 * @see GBPTreeGenericCountsStore
 */
public class GBPTreeRelationshipGroupDegreesStore extends GBPTreeGenericCountsStore implements RelationshipGroupDegreesStore
{
    private static final String NAME = "Relationship group degrees store";
    static final byte TYPE_DEGREE = (byte) 3;

    public GBPTreeRelationshipGroupDegreesStore( PageCache pageCache, Path file, FileSystemAbstraction fileSystem,
            RecoveryCleanupWorkCollector recoveryCollector, DegreesRebuilder rebuilder, DatabaseReadOnlyChecker readOnlyChecker,
            Monitor monitor, String databaseName, int maxCacheSize, InternalLogProvider userLogProvider,
            CursorContextFactory contextFactory ) throws IOException
    {
        super( pageCache, file, fileSystem, recoveryCollector, new RebuilderWrapper( rebuilder ), readOnlyChecker, NAME, monitor, databaseName,
                maxCacheSize, userLogProvider, contextFactory );
    }

    @Override
    public Updater apply( long txId, CursorContext cursorContext )
    {
        CountUpdater updater = updater( txId, cursorContext );
        return updater != null ? new DegreeUpdater( updater ) : NO_OP_UPDATER;
    }

    public Updater directApply( CursorContext cursorContext ) throws IOException
    {
        return new DegreeUpdater( directUpdater( true, cursorContext ) );
    }

    @Override
    public long degree( long groupId, RelationshipDirection direction, CursorContext cursorContext )
    {
        return read( degreeKey( groupId, direction ), cursorContext );
    }

    @Override
    public void accept( GroupDegreeVisitor visitor, CursorContext cursorContext )
    {
        visitAllCounts( ( key, count ) -> visitor.degree( groupIdOf( key ), directionOf( key ), count ), cursorContext );
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

    /**
     * Public utility method for instantiating a {@link CountsKey} for a degree.
     *
     * Key data layout for this type:
     * <pre>
     * first:  [gggg,gggg][gggg,gggg][gggg,gggg][gggg,gggg] [gggg,gggg][gggg,gggg][gggg,gggg][gggg,ggdd]
     *         g: relationship group id, {@link RelationshipGroupRecord#getId()}
     *         d: {@link RelationshipDirection#id()}
     * second: 0
     * </pre>
     *
     * @param groupId relationship group ID.
     * @param direction direction for the relationship chain.
     * @return a {@link CountsKey for the relationship chain (group+direction). The returned key can be put into {@link Map maps} and similar.
     */
    static CountsKey degreeKey( long groupId, RelationshipDirection direction )
    {
        return new CountsKey( TYPE_DEGREE, groupId << 2 | direction.id(), 0 );
    }

    static String keyToString( CountsKey key )
    {
        if ( key.type == TYPE_DEGREE )
        {
            return format( "Degree[groupId:%d, direction:%s]", groupIdOf( key ), directionOf( key ) );
        }
        throw new IllegalArgumentException( "Unknown type " + key.type );
    }

    private static RelationshipDirection directionOf( CountsKey key )
    {
        return RelationshipDirection.ofId( (int) (key.first & 0x3) );
    }

    private static long groupIdOf( CountsKey key )
    {
        return key.first >> 2;
    }

    public static void dump( PageCache pageCache, Path file, PrintStream out, CursorContextFactory contextFactory ) throws IOException
    {
        GBPTreeGenericCountsStore.dump( pageCache, file, out, DEFAULT_DATABASE_NAME, NAME, contextFactory, GBPTreeRelationshipGroupDegreesStore::keyToString );
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
        void rebuild( Updater updater, CursorContext cursorContext, MemoryTracker memoryTracker );

        long lastCommittedTxId();
    }

    private static class RebuilderWrapper implements Rebuilder
    {
        private final DegreesRebuilder rebuilder;

        RebuilderWrapper( DegreesRebuilder rebuilder )
        {
            this.rebuilder = rebuilder;
        }

        @Override
        public void rebuild( CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker )
        {
            rebuilder.rebuild( new DegreeUpdater( updater ), cursorContext, memoryTracker );
        }

        @Override
        public long lastCommittedTxId()
        {
            return rebuilder.lastCommittedTxId();
        }
    }

    public static class EmptyDegreesRebuilder implements DegreesRebuilder
    {
        private final long lastTxId;

        public EmptyDegreesRebuilder( long lastTxId )
        {
            this.lastTxId = lastTxId;
        }

        @Override
        public void rebuild( RelationshipGroupDegreesStore.Updater updater, CursorContext cursorContext, MemoryTracker memoryTracker )
        {
        }

        @Override
        public long lastCommittedTxId()
        {
            return lastTxId;
        }
    }
}
