/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.kernel.impl.util.concurrent.WaitStrategy;

public enum ResourceTypes implements Locks.ResourceType
{
    NODE        (0, LockWaitStrategies.INCREMENTAL_BACKOFF),
    RELATIONSHIP(1, LockWaitStrategies.INCREMENTAL_BACKOFF),

    GRAPH_PROPS (2, LockWaitStrategies.INCREMENTAL_BACKOFF),

    SCHEMA      (3, LockWaitStrategies.INCREMENTAL_BACKOFF),
    INDEX_ENTRY (4, LockWaitStrategies.INCREMENTAL_BACKOFF),

    LEGACY_INDEX(5, LockWaitStrategies.INCREMENTAL_BACKOFF)
    ;

    private final static Map<Integer, Locks.ResourceType> idToType = new HashMap<>();
    static
    {
        for ( ResourceTypes resourceTypes : ResourceTypes.values() )
        {
            idToType.put( resourceTypes.typeId, resourceTypes );
        }
    }

    private final int typeId;

    private final WaitStrategy waitStrategy;

    private ResourceTypes( int typeId, WaitStrategy waitStrategy )
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

    public static long legacyIndexResourceId( String name, String key )
    {
        return (long)name.hashCode() << 32 | key.hashCode();
    }

    public static long indexEntryResourceId( long labelId, long propertyKeyId, String propertyValue )
    {
        long result = labelId;
        result = 31 * result + propertyKeyId;
        result = 31 * result + propertyValue.hashCode();
        return result;
    }

    public static long graphPropertyResource()
    {
        return 0l;
    }

    public static long schemaResource()
    {
        return 0l;
    }

    public static Locks.ResourceType fromId( int typeId )
    {
        return idToType.get( typeId );
    }
}
