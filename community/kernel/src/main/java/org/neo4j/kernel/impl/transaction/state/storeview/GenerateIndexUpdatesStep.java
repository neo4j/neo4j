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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.PrimitiveArrays.intsToLongs;

public class GenerateIndexUpdatesStep<CURSOR extends StorageEntityScanCursor<?>> extends ProcessorStep<long[]>
{
    private static final String TRACER_TAG_PREFIX = "indexPopulationStep:";

    private final StorageReader reader;
    private final IntPredicate propertyKeyIdFilter;
    private final EntityScanCursorBehaviour<CURSOR> entityCursorBehaviour;
    private final long[] relevantTokenIds;
    private final Visitor<List<EntityUpdates>,? extends Exception> propertyUpdatesVisitor;
    private final Visitor<List<EntityTokenUpdate>,? extends Exception> tokenUpdatesVisitor;
    private final boolean gatherTokenUpdates;
    private final boolean gatherPropertyUpdates;
    private final LongFunction<Lock> lockFunction;
    private final boolean alsoWrite;
    private final MemoryTracker memoryTracker;
    private final long maxBatchSizeBytes;

    public GenerateIndexUpdatesStep( StageControl control, Configuration config, StorageReader reader, IntPredicate propertyKeyIdFilter,
            EntityScanCursorBehaviour<CURSOR> entityCursorBehaviour, int[] entityTokenIdFilter,
            Visitor<List<EntityUpdates>,? extends Exception> propertyUpdatesVisitor, Visitor<List<EntityTokenUpdate>,? extends Exception> tokenUpdatesVisitor,
            LongFunction<Lock> lockFunction, int parallelism, long maxBatchSizeBytes, boolean alsoWrite, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        super( control, "generate updates", config, parallelism, cacheTracer );
        this.reader = reader;
        this.propertyKeyIdFilter = propertyKeyIdFilter;
        this.entityCursorBehaviour = entityCursorBehaviour;
        this.relevantTokenIds = intsToLongs( entityTokenIdFilter );
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.tokenUpdatesVisitor = tokenUpdatesVisitor;
        this.gatherPropertyUpdates = propertyUpdatesVisitor != null;
        this.gatherTokenUpdates = tokenUpdatesVisitor != null;
        this.lockFunction = lockFunction;
        this.alsoWrite = alsoWrite;
        this.memoryTracker = memoryTracker;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
    }

    @Override
    protected void process( long[] entityIds, BatchSender sender, PageCursorTracer cursorTracer ) throws Exception
    {
        GeneratedIndexUpdates updates = new GeneratedIndexUpdates( gatherPropertyUpdates, gatherTokenUpdates );
        try ( CURSOR nodeCursor = entityCursorBehaviour.allocateEntityScanCursor( cursorTracer );
              StoragePropertyCursor propertyCursor = reader.allocatePropertyCursor( cursorTracer, memoryTracker ) )
        {
            for ( long entityId : entityIds )
            {
                try ( Lock ignored = lockFunction.apply( entityId ) )
                {
                    nodeCursor.single( entityId );
                    if ( nodeCursor.next() )
                    {
                        generateUpdates( updates, nodeCursor, propertyCursor );
                        if ( updates.propertiesByteSize > maxBatchSizeBytes )
                        {
                            batchDone( updates, sender );
                            updates = new GeneratedIndexUpdates( gatherPropertyUpdates, gatherTokenUpdates );
                        }
                    }
                }
            }
        }
        if ( !updates.isEmpty() )
        {
            batchDone( updates, sender );
        }
    }

    private void batchDone( GeneratedIndexUpdates updates, BatchSender sender ) throws Exception
    {
        if ( alsoWrite )
        {
            updates.accept( propertyUpdatesVisitor, tokenUpdatesVisitor );
        }
        else
        {
            sender.send( updates );
        }
    }

    private void generateUpdates( GeneratedIndexUpdates updates, CURSOR entityCursor, StoragePropertyCursor propertyCursor )
    {
        long[] tokens = entityCursorBehaviour.readTokens( entityCursor );
        if ( tokens.length == 0 )
        {
            // This entity has no tokens at all
            return;
        }

        if ( gatherTokenUpdates )
        {
            updates.tokenUpdates.add( EntityTokenUpdate.tokenChanges( entityCursor.entityReference(), EMPTY_LONG_ARRAY, tokens ) );
        }

        if ( gatherPropertyUpdates && containsAnyEntityToken( relevantTokenIds, tokens ) )
        {
            readRelevantProperties( entityCursor, propertyCursor, tokens, updates );
        }
    }

    void readRelevantProperties( CURSOR cursor, StoragePropertyCursor propertyCursor, long[] tokens, GeneratedIndexUpdates indexUpdates )
    {
        if ( !cursor.hasProperties() )
        {
            return;
        }
        boolean hasRelevantProperty = false;
        cursor.properties( propertyCursor );
        EntityUpdates.Builder updates = EntityUpdates.forEntity( cursor.entityReference(), true ).withTokens( tokens );
        while ( propertyCursor.next() )
        {
            int propertyKeyId = propertyCursor.propertyKey();
            if ( propertyKeyIdFilter.test( propertyKeyId ) )
            {
                // This entity has a property of interest to us
                Value value = propertyCursor.propertyValue();
                // No need to validate values before passing them to the updater since the index implementation
                // is allowed to fail in which ever way it wants to. The result of failure will be the same as
                // a failed validation, i.e. population FAILED.
                updates.added( propertyKeyId, value );
                hasRelevantProperty = true;
                indexUpdates.propertiesByteSize += value.estimatedHeapUsage();
            }
        }
        if ( hasRelevantProperty )
        {
            indexUpdates.propertyUpdates.add( updates.build() );
        }
    }

    @Override
    protected String buildCursorTracerName()
    {
        return TRACER_TAG_PREFIX + name();
    }

    static boolean containsAnyEntityToken( long[] entityTokenFilter, long... entityTokens )
    {
        for ( long candidate : entityTokens )
        {
            if ( ArrayUtils.contains( entityTokenFilter, Math.toIntExact( candidate ) ) )
            {
                return true;
            }
        }
        return false;
    }

    static class GeneratedIndexUpdates
    {
        private final List<EntityUpdates> propertyUpdates;
        private final List<EntityTokenUpdate> tokenUpdates;
        private long propertiesByteSize;

        GeneratedIndexUpdates( boolean gatherPropertyUpdates, boolean gatherTokenUpdates )
        {
            propertyUpdates = gatherPropertyUpdates ? new ArrayList<>() : null;
            tokenUpdates = gatherTokenUpdates ? new ArrayList<>() : null;
        }

        boolean isEmpty()
        {
            return (propertyUpdates == null || propertyUpdates.isEmpty()) && (tokenUpdates == null || tokenUpdates.isEmpty());
        }

        void accept( Visitor<List<EntityUpdates>,? extends Exception> propertyUpdatesVisitor,
                Visitor<List<EntityTokenUpdate>,? extends Exception> tokenUpdatesVisitor ) throws Exception
        {
            if ( propertyUpdatesVisitor != null )
            {
                propertyUpdatesVisitor.visit( propertyUpdates );
            }

            if ( tokenUpdatesVisitor != null )
            {
                tokenUpdatesVisitor.visit( tokenUpdates );
            }
        }
    }
}
