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
package org.neo4j.internal.id;

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;

/**
 * Wraps {@link IdGenerator} so that ids can be freed using reuse marker at safe points in time, after all transactions
 * which were active at the time of freeing, have been closed.
 */
public class BufferingIdGeneratorFactory implements IdGeneratorFactory
{
    private static final int MAX_QUEUED_BUFFERS = 20;

    private final Map<IdType, BufferingIdGenerator> overriddenIdGenerators = new HashMap<>();
    private Supplier<IdController.IdFreeCondition> boundaries;
    private MemoryTracker memoryTracker;
    private final IdGeneratorFactory delegate;
    private final Deque<IdBuffers> bufferQueue = new ArrayDeque<>();

    public BufferingIdGeneratorFactory( IdGeneratorFactory delegate )
    {
        this.delegate = delegate;
    }

    public void initialize( Supplier<IdController.IdFreeCondition> conditionSupplier, MemoryTracker memoryTracker )
    {
        boundaries = conditionSupplier;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public IdGenerator open( PageCache pageCache, Path filename, IdType idType, LongSupplier highIdScanner, long maxId, DatabaseReadOnlyChecker readOnlyChecker,
            Config config, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions, IdSlotDistribution slotDistribution ) throws IOException
    {
        assert boundaries != null : "Factory needs to be initialized before usage";

        IdGenerator generator =
                delegate.open( pageCache, filename, idType, highIdScanner, maxId, readOnlyChecker, config, cursorContext, openOptions, slotDistribution );
        return wrapAndKeep( idType, generator );
    }

    @Override
    public IdGenerator create( PageCache pageCache, Path filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
            DatabaseReadOnlyChecker readOnlyChecker, Config config, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution ) throws IOException
    {
        IdGenerator idGenerator =
                delegate.create( pageCache, filename, idType, highId, throwIfFileExists, maxId, readOnlyChecker, config, cursorContext, openOptions,
                        slotDistribution );
        return wrapAndKeep( idType, idGenerator );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        IdGenerator generator = overriddenIdGenerators.get( idType );
        return generator != null ? generator : delegate.get( idType );
    }

    @Override
    public void visit( Consumer<IdGenerator> visitor )
    {
        overriddenIdGenerators.values().forEach( visitor );
    }

    @Override
    public void clearCache( CursorContext cursorContext )
    {
        delegate.clearCache( cursorContext );
    }

    @Override
    public Collection<Path> listIdFiles()
    {
        return delegate.listIdFiles();
    }

    private IdGenerator wrapAndKeep( IdType idType, IdGenerator generator )
    {
        BufferingIdGenerator bufferingGenerator = new BufferingIdGenerator( generator, memoryTracker );
        overriddenIdGenerators.put( idType, bufferingGenerator );
        return bufferingGenerator;
    }

    public synchronized void maintenance( CursorContext cursorContext )
    {
        if ( bufferQueue.size() < MAX_QUEUED_BUFFERS )
        {
            // Gather currently pending deleted IDs
            List<IdBuffer> buffers = new ArrayList<>();
            overriddenIdGenerators.values().forEach( generator -> generator.collectBufferedIds( buffers ) );
            if ( !buffers.isEmpty() )
            {
                bufferQueue.offer( new IdBuffers( buffers, boundaries.get() ) );
            }
        }
        // else there's one or more open (long-lived) transactions blocking freeing of IDs and it's therefore unnecessary
        // to pile on more buffers including their transaction snapshots to the buffer queue.

        // Check and free deleted IDs that are safe to free
        IdBuffers candidate;
        while ( (candidate = bufferQueue.peek()) != null )
        {
            if ( candidate.idFreeCondition.eligibleForFreeing() )
            {
                bufferQueue.remove();
                candidate.free( cursorContext );
            }
            else
            {
                break;
            }
        }
        overriddenIdGenerators.values().forEach( generator -> generator.maintenance( cursorContext ) );
    }

    /**
     * Contains deleted IDs from multiple ID generators, with an attached condition to know when to free it
     */
    static class IdBuffers
    {
        private final List<IdBuffer> buffers;
        private final IdController.IdFreeCondition idFreeCondition;

        IdBuffers( List<IdBuffer> buffers, IdController.IdFreeCondition idFreeCondition )
        {
            this.buffers = buffers;
            this.idFreeCondition = idFreeCondition;
        }

        void free( CursorContext cursorContext )
        {
            for ( IdBuffer buffer : buffers )
            {
                buffer.free( cursorContext );
            }
        }
    }

    static class IdBuffer
    {
        private final IdGenerator idGenerator;
        private final HeapTrackingLongArrayList ids;

        IdBuffer( IdGenerator idGenerator, HeapTrackingLongArrayList ids )
        {
            this.idGenerator = idGenerator;
            this.ids = ids;
        }

        void free( CursorContext cursorContext )
        {
            try ( IdGenerator.Marker marker = idGenerator.marker( cursorContext ) )
            {
                try ( PrimitiveLongResourceIterator idIterator = ids.iterator() )
                {
                    while ( idIterator.hasNext() )
                    {
                        long id = idIterator.next();
                        marker.markFree( idFromCombinedId( id ), numberOfIdsFromCombinedId( id ) );
                    }
                }
            }
            finally
            {
                ids.close();
            }
        }
    }
}
