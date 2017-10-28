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

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.hashing.HashFunction;
import org.neo4j.helpers.Strings;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public enum ResourceTypes implements ResourceType
{
    NODE( 0, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP( 1, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    GRAPH_PROPS( 2, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    // SCHEMA resource type had typeId 3 - skip it to avoid resource types conflicts
    INDEX_ENTRY( 4, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    EXPLICIT_INDEX( 5, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    LABEL( 6, LockWaitStrategies.INCREMENTAL_BACKOFF ),
    RELATIONSHIP_TYPE( 7, LockWaitStrategies.INCREMENTAL_BACKOFF );

    private static final boolean useStrongHashing =
            FeatureToggles.flag( ResourceTypes.class, "useStrongHashing", false );

    private static final Map<Integer,ResourceType> idToType = new HashMap<>();
    private static final HashFunction indexEntryHash_2_2_0 = HashFunction.xorShift32();
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
     * The index entry hashing method used for entries in explicit indexes.
     */
    public static long explicitIndexResourceId( String name, String key )
    {
        return (long) name.hashCode() << 32 | key.hashCode();
    }

    /**
     * This is the schema index entry hashing method used since 2.2.0 and onwards.
     * <p>
     * Use the {@link ResourceTypes#useStrongHashing} feature toggle to use a stronger hash function, which will become
     * the default in a future release. <strong>Note</strong> that changing this hash function is effectively a
     * clustering protocol change in HA setups. Causal cluster setups are unaffected because followers do not take any
     * locks on the cluster leader.
     */
    public static long indexEntryResourceId( long labelId, IndexQuery.ExactPredicate... predicates )
    {
        if ( !useStrongHashing )
        {
            // Default
            return indexEntryResourceId_2_2_0( labelId, predicates );
        }
        else
        {
            // Opt-in
            return indexEntryResourceId_4_x( labelId, predicates );
        }
    }

    static long indexEntryResourceId_2_2_0( long labelId, IndexQuery.ExactPredicate[] predicates )
    {
        return indexEntryResourceId_2_2_0( labelId, predicates, 0 );
    }

    private static long indexEntryResourceId_2_2_0( long labelId, IndexQuery.ExactPredicate[] predicates, int i )
    {
        int propertyKeyId = predicates[i].propertyKeyId();
        Value value = predicates[i].value();
        // Note:
        // It is important that single-property indexes only hash with this particular call; no additional hashing!
        long hash = indexEntryResourceId_2_2_0( labelId, propertyKeyId, stringOf( value ) );
        i++;
        if ( i < predicates.length )
        {
            hash = hash( hash + indexEntryResourceId_2_2_0( labelId, predicates, i ) );
        }
        return hash;
    }

    private static long indexEntryResourceId_2_2_0( long labelId, long propertyKeyId, String propertyValue )
    {
        long hob = hash( labelId + hash( propertyKeyId ) );
        hob <<= 32;
        return hob + propertyValue.hashCode();
    }

    private static String stringOf( Value value )
    {
        if ( value != null && value != Values.NO_VALUE )
        {
            return Strings.prettyPrint( value.asObject() );
        }
        return "";
    }

    private static int hash( long value )
    {
        return indexEntryHash_2_2_0.hashSingleValueToInt( value );
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
     * This is a stronger, full 64-bit hashing method for schema index entries that we should use by default in a
     * future release, where we will also upgrade the HA protocol version. Currently this is indicated by the "4_x"
     * name suffix, but any version where the HA protocol version changes anyway would be just as good an opportunity.
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
            Value v = predicate.value();
            Object value = v.asObject();
            Class<?> type = value.getClass();

            hash = indexEntryHash_4_x.update( hash, propertyKeyId );

            if ( type == String.class )
            {
                String str = (String) value;
                int length = str.length();

                hash = indexEntryHash_4_x.update( hash, length );

                for ( int i = 0; i < length; i++ )
                {
                    hash = indexEntryHash_4_x.update( hash, str.charAt( i ) );
                }
            }
            else if ( type.isArray() )
            {
                int length = Array.getLength( value );
                Class<?> componentType = type.getComponentType();

                hash = indexEntryHash_4_x.update( hash, length );

                if ( componentType == String.class )
                {
                    for ( int i = 0; i < length; i++ )
                    {
                        String str = (String) Array.get( value, i );
                        int len = str.length();

                        hash = indexEntryHash_4_x.update( hash, len );

                        for ( int j = 0; j < len; j++ )
                        {
                            hash = indexEntryHash_4_x.update( hash, str.charAt( j ) );
                        }
                    }
                }
                else if ( componentType == Double.TYPE )
                {
                    for ( int i = 0; i < length; i++ )
                    {
                        hash = indexEntryHash_4_x.update(
                                hash, Double.doubleToLongBits( Array.getDouble( value, i ) ) );
                    }
                }
                else if ( componentType == Boolean.TYPE )
                {
                    for ( int i = 0; i < length; i++ )
                    {
                        hash = indexEntryHash_4_x.update( hash, Boolean.hashCode( Array.getBoolean( value, i ) ) );
                    }
                }
                else if ( componentType == Character.TYPE )
                {
                    for ( int i = 0; i < length; i++ )
                    {
                        hash = indexEntryHash_4_x.update( hash, Array.getChar( value, i ) );
                    }
                }
                else
                {
                    for ( int i = 0; i < length; i++ )
                    {
                        hash = indexEntryHash_4_x.update( hash, ((Number) Array.get( value, i )).longValue() );
                    }
                }
            }
            else if ( type == Double.class )
            {
                hash = indexEntryHash_4_x.update( hash, Double.doubleToLongBits( (Double) value ) );
            }
            else if ( type == Boolean.class )
            {
                hash = indexEntryHash_4_x.update( hash, value.hashCode() );
            }
            else if ( type == Character.class )
            {
                hash = indexEntryHash_4_x.update( hash, (char) value );
            }
            else
            {
                hash = indexEntryHash_4_x.update( hash, ((Number) value).longValue() );
            }
        }

        return indexEntryHash_4_x.finalise( hash );
    }
}
