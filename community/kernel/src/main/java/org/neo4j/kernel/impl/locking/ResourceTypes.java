/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

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

    /**
     * This is the schema index entry hashing method used since 2.2.0 and onwards.
     */
    public static long indexEntryResourceId( long labelId, long propertyKeyId, String propertyValue )
    {
        long hob = hash( labelId + hash( propertyKeyId ) );
        hob <<= 32;
        return hob + propertyValue.hashCode();

        // The previous schema index entry hashing method used up until and
        // including Neo4j 2.1.x looks like the following:
        //
        //   long result = labelId;
        //   result = 31 * result + propertyKeyId;
        //   result = 31 * result + propertyValue.hashCode();
        //   return result;
        //
        // It was replaced because it was prone to collisions. I left it in
        // this comment in case we need it for supporting rolling upgrades.
        // This comment can be deleted once RU from 2.1 to 2.2 is no longer a
        // concern.
    }

    private static int hash( long value )
    {
        return DEFAULT_HASHING.hash( value );
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
