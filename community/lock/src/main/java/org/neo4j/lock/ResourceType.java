/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.lock;

import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
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
 *
 *     <dt>{@link #NODE_RELATIONSHIPS} - Node id</dt>
 *     <dd>Lock the relationships side of a node to prevent concurrent updates. Block-format only</dd>
 *
 *     <dt>{@link #NODE_LOCAL_IDS} - Node id</dt>
 *     <dd>Lock the node-local relationship ids of a node to prevent concurrent updates. Block-format only</dd>
 * </dl>
 */
public enum ResourceType {
    NODE(0),
    RELATIONSHIP(1),
    INDEX_ENTRY(2),
    LABEL(3),
    RELATIONSHIP_TYPE(4),
    SCHEMA_NAME(5),
    RELATIONSHIP_DELETE(6),
    NODE_RELATIONSHIP_GROUP_DELETE(7),
    DEGREES(8),
    RELATIONSHIP_GROUP(9),
    PAGE(10),
    NODE_RELATIONSHIPS(11),
    NODE_LOCAL_IDS(12);

    private static final ImmutableIntObjectMap<ResourceType> idToType;

    static {
        var mapping = new IntObjectHashMap<ResourceType>();
        for (var type : ResourceType.values()) {
            mapping.put(type.typeId, type);
        }
        idToType = mapping.toImmutable();
    }

    private final int typeId;

    ResourceType(int typeId) {
        this.typeId = typeId;
    }

    public int typeId() {
        return typeId;
    }

    public static ResourceType fromId(int typeId) {
        return idToType.get(typeId);
    }
}
