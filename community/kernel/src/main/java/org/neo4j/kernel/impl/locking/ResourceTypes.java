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
package org.neo4j.kernel.impl.locking;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import org.neo4j.hashing.HashFunction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;
import org.neo4j.values.storable.Value;

public enum ResourceTypes implements ResourceType
{
    NODE( 0, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP( 1, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    GRAPH_PROPS( 2, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    // SCHEMA resource type had typeId 3 - skip it to avoid resource types conflicts
    INDEX_ENTRY( 4, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    // EXPLICIT INDEX resource had type id 5 - skip it to avoid resource types conflicts
    LABEL( 6, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP_TYPE( 7, LockWaitStrategies.INCREMENTAL_BACKOFF );

    private static final MutableIntObjectMap<ResourceType> idToType = new IntObjectHashMap<>();
    private static final HashFunction indexEntryHash_4_x = HashFunction.incrementalXXH64();

    static
    {
        for ( ResourceTypes resourceTypes : ResourceTypes.values() )
        {
            idToType.put( resourceTypes.typeId, resourceTypes );
        }
    }

    private final int typeId;

    private final WaitStrategy waitStrategy;

    ResourceTypes( int typeId, WaitStrategy waitStrategy )
    {
        this.typeId = typeId;
        this.waitStrategy = waitStrategy;
    }

    @Override
    public int typeId()
    {
        return typeId;
    }

    @Override
    public WaitStrategy waitStrategy()
    {
        return waitStrategy;
    }

    /**
     * Produces a 64-bit hashcode for locking index entries.
     */
    public static long indexEntryResourceId( long labelId, IndexQuery.ExactPredicate... predicates )
    {
        return indexEntryResourceId_4_x( labelId, predicates );
    }

    public static long graphPropertyResource()
    {
        return 0L;
    }

    public static ResourceType fromId( int typeId )
    {
        return idToType.get( typeId );
    }

    /**
     * This is a stronger, full 64-bit hashing method for schema index entries.
     *
     * @see HashFunction#incrementalXXH64()
     */
    static long indexEntryResourceId_4_x( long labelId, IndexQuery.ExactPredicate[] predicates )
    {
        long hash = indexEntryHash_4_x.initialise( 0x0123456789abcdefL );

        hash = indexEntryHash_4_x.update( hash, labelId );

        for ( IndexQuery.ExactPredicate predicate : predicates )
        {
            int propertyKeyId = predicate.propertyKeyId();
            hash = indexEntryHash_4_x.update( hash, propertyKeyId );
            Value value = predicate.value();
            hash = value.updateHash( indexEntryHash_4_x, hash );
        }

        return indexEntryHash_4_x.finalise( hash );
    }
}
