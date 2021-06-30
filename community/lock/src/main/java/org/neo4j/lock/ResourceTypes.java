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
package org.neo4j.lock;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

/**
 * Locking types.
 *
 * To avoid deadlocks, they should be taken in the following order
 * <dl>
 *     <dt>{@link #LABEL} or {@link #RELATIONSHIP_TYPE} - Token id</dt>
 *     <dd>Schema locks, will lock indexes and constraints on the particular label or relationship type.</dd>
 *
 *     <dt>{@link #SCHEMA_NAME} - Schema name (XXH64 hashed)</dt>
 *     <dd>
 *         Lock a schema name to avoid duplicates. Note, collisions are possible since we hash the string, but this only affect concurrency and not correctness.
 *     </dd>
 *
 *     <dt>{@link #NODE_RELATIONSHIP_GROUP_DELETE} - Node id</dt>
 *     <dd>
 *         Lock taken on a node during the transaction creation phase to prevent deletion of said node and/or relationship group.
 *         This is different from the {@link #NODE} to allow concurrent label and property changes together with relationship modifications.
 *     </dd>
 *
 *     <dt>{@link #NODE} - Node id</dt>
 *     <dd>
 *         Lock on a node, used to prevent concurrent updates to the node records, i.e. add/remove label, set property, add/remove relationship.
 *         Note that changing relationships will only require a lock on the node if the head of the relationship chain/relationship group chain
 *         must be updated, since that is the only data part of the node record.
 *     </dd>
 *
 *     <dt>{@link #DEGREES} - Node id</dt>
 *     <dd>
 *         Used to lock nodes to avoid concurrent label changes with relationship addition/deletion. This would otherwise lead to inconsistent count store.
 *     </dd>
 *
 *     <dt>{@link #RELATIONSHIP_DELETE} - Relationship id</dt>
 *     <dd>Lock a relationship for exclusive access during deletion.</dd>
 *
 *     <dt>{@link #RELATIONSHIP_GROUP} - Node id</dt>
 *     <dd>
 *         Lock the full relationship group chain for a given node(dense). This will not lock the node in contrast to {@link #NODE_RELATIONSHIP_GROUP_DELETE}.
 *     </dd>
 *
 *     <dt>{@link #RELATIONSHIP} - Relationship id</dt>
 *     <dd>Lock on a relationship, or more specifically a relationship record, to prevent concurrent updates.</dd>
 * </dl>
 */
public enum ResourceTypes implements ResourceType
{
    NODE( 0 ),
    RELATIONSHIP( 1 ),
    // GRAPH_PROPS( 2, LockWaitStrategies.INCREMENTAL_BACKOFF ) - skip it to avoid resource types conflicts
    // SCHEMA resource type had typeId 3 - skip it to avoid resource types conflicts
    INDEX_ENTRY( 4 ),
    // EXPLICIT INDEX resource had type id 5 - skip it to avoid resource types conflicts
    LABEL( 6 ),
    RELATIONSHIP_TYPE( 7 ),
    SCHEMA_NAME( 8 ),
    RELATIONSHIP_GROUP( 9 ),
    RELATIONSHIP_DELETE( 10 ),
    NODE_RELATIONSHIP_GROUP_DELETE( 11 ),
    DEGREES( 12 );

    private static final MutableIntObjectMap<ResourceType> idToType = new IntObjectHashMap<>();

    static
    {
        for ( ResourceTypes resourceTypes : ResourceTypes.values() )
        {
            idToType.put( resourceTypes.typeId, resourceTypes );
        }
    }

    private final int typeId;

    ResourceTypes( int typeId )
    {
        this.typeId = typeId;
    }

    @Override
    public int typeId()
    {
        return typeId;
    }

    public static ResourceType fromId( int typeId )
    {
        return idToType.get( typeId );
    }
}
