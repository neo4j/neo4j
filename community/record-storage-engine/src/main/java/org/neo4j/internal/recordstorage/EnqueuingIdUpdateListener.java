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
package org.neo4j.internal.recordstorage;

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.util.concurrent.WorkSync;

class EnqueuingIdUpdateListener implements IdUpdateListener
{
    private final EnumMap<IdType,ChangedIds> idUpdates;
    private final Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs;
    private final PageCacheTracer pageCacheTracer;

    EnqueuingIdUpdateListener( Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs, PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
        this.idUpdates = new EnumMap<>( IdType.class );
        this.idGeneratorWorkSyncs = idGeneratorWorkSyncs;
    }

    @Override
    public void markIdAsUsed( IdType idType, IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
    {
        idUpdates.computeIfAbsent( idType, k -> new ChangedIds() ).addUsedId( id );
    }

    @Override
    public void markIdAsUnused( IdType idType, IdGenerator idGenerator, long id, PageCursorTracer cursorTracer )
    {
        idUpdates.computeIfAbsent( idType, k -> new ChangedIds() ).addUnusedId( id );
    }

    @Override
    public void close() throws Exception
    {
        // Run through the id changes and apply them, or rather apply them asynchronously.
        // This allows multiple concurrent threads applying batches of transactions to help each other out so that
        // there's a higher chance that changes to different id types can be applied in parallel.
        for ( Map.Entry<IdType,ChangedIds> idChanges : idUpdates.entrySet() )
        {
            ChangedIds unit = idChanges.getValue();
            unit.applyAsync( idGeneratorWorkSyncs.get( idChanges.getKey() ), pageCacheTracer );
        }

        // Wait for all id updates to complete
        for ( Map.Entry<IdType,ChangedIds> idChanges : idUpdates.entrySet() )
        {
            ChangedIds unit = idChanges.getValue();
            unit.awaitApply();
        }
    }
}
