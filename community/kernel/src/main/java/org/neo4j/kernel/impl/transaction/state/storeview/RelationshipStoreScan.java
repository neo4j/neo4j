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

import java.util.List;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.lock.LockType.SHARED;

/**
 * Scan the relationship store and produce {@link EntityUpdates updates for indexes} and/or {@link EntityTokenUpdate updates for relationship type index}
 * depending on which {@link Visitor visitors} that are used.
 */
public class RelationshipStoreScan<FAILURE extends Exception> extends PropertyAwareEntityStoreScan<StorageRelationshipScanCursor,FAILURE>
{
    public RelationshipStoreScan( Config config, StorageReader storageReader, LockService locks,
            @Nullable Visitor<List<EntityTokenUpdate>,FAILURE> relationshipTypeUpdateVisitor,
            @Nullable Visitor<List<EntityUpdates>,FAILURE> propertyUpdatesVisitor,
            int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter, boolean parallelWrite,
            JobScheduler scheduler, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( config, storageReader, storageReader.relationshipsGetCount(), relationshipTypeIds, propertyKeyIdFilter, relationshipTypeUpdateVisitor,
                propertyUpdatesVisitor, id -> locks.acquireRelationshipLock( id, SHARED ), new RelationshipCursorBehaviour( storageReader ), parallelWrite,
                scheduler, cacheTracer, memoryTracker );
    }
}
