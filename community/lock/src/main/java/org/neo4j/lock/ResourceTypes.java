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
 * Generic Locking types. See storage engine specific resource types for details
 */
public enum ResourceTypes implements ResourceType {
    NODE(0),
    RELATIONSHIP(1),
    INDEX_ENTRY(2),
    LABEL(3),
    RELATIONSHIP_TYPE(4),
    SCHEMA_NAME(5),
    RELATIONSHIP_DELETE(6),
    NODE_RELATIONSHIP_GROUP_DELETE(7),
    DEGREES(8),
    RELATIONSHIP_GROUP(9);

    private static final MutableIntObjectMap<ResourceType> idToType = new IntObjectHashMap<>();

    static {
        for (ResourceTypes resourceTypes : ResourceTypes.values()) {
            idToType.put(resourceTypes.typeId, resourceTypes);
        }
    }

    private final int typeId;

    ResourceTypes(int typeId) {
        this.typeId = typeId;
    }

    @Override
    public int typeId() {
        return typeId;
    }

    public static ResourceType fromId(int typeId) {
        return idToType.get(typeId);
    }
}
