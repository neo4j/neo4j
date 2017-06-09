/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;

import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;

public enum ResourceTypes implements ResourceType
{
    NODE( 0, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP( 1, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    GRAPH_PROPS( 2, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    INDEX_ENTRY( 3, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    LEGACY_INDEX( 4, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    LABEL( 5, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP_TYPE( 6, LockWaitStrategies.INCREMENTAL_BACKOFF );

    private static final Map<Integer, ResourceType> idToType = new HashMap<>();
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

    public static long legacyIndexResourceId( String name, String key )
    {
        return (long)name.hashCode() << 32 | key.hashCode();
    }

    public static long indexEntryResourceId( long labelId, IndexQuery.ExactPredicate... predicates )
    {
        return indexEntryResourceId( labelId, predicates, 0 );
    }

    private static long indexEntryResourceId( long labelId, IndexQuery.ExactPredicate[] predicates, int i )
    {
        int propertyKeyId = predicates[i].propertyKeyId();
        Object value = predicates[i].value();
        // Note:
        // It is important that single-property indexes only hash with this particular call; no additional hashing!
        long hash = indexEntryResourceId( labelId, propertyKeyId, stringOf( propertyKeyId, value ) );
        i++;
        if ( i < predicates.length )
        {
            hash = hash( hash + indexEntryResourceId( labelId, predicates, i ) );
        }
        return hash;
    }

    private static long indexEntryResourceId( long labelId, long propertyKeyId, String propertyValue )
    {
        long hob = hash( labelId + hash( propertyKeyId ) );
        hob <<= 32;
        return hob + propertyValue.hashCode();
    }

    private static String stringOf( int propertyKeyId, Object value )
    {
        if ( null != value )
        {
            DefinedProperty property = Property.property( propertyKeyId, value );
            return property.valueAsString();
        }
        return "";
    }

    private static int hash( long value )
    {
        return DEFAULT_HASHING.hash( value );
    }

    public static long graphPropertyResource()
    {
        return 0L;
    }

    public static ResourceType fromId( int typeId )
    {
        return idToType.get( typeId );
    }
}
