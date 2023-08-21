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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.id.IdType;

public enum RecordIdType implements IdType {
    NODE(true),
    RELATIONSHIP(true),
    PROPERTY(true),
    STRING_BLOCK(true),
    ARRAY_BLOCK(true),
    PROPERTY_KEY_TOKEN_NAME(false),
    RELATIONSHIP_TYPE_TOKEN_NAME(false),
    LABEL_TOKEN_NAME(false),
    NODE_LABELS(false),
    RELATIONSHIP_GROUP(true);

    private final boolean highActivity;

    RecordIdType(boolean highActivity) {
        this.highActivity = highActivity;
    }

    @Override
    public boolean highActivity() {
        return highActivity;
    }
}
