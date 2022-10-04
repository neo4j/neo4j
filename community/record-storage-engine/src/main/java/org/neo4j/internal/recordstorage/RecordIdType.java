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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.id.IdType;

public enum RecordIdType implements IdType {
    NODE(true, false),
    RELATIONSHIP(true, false),
    PROPERTY(true, false),
    STRING_BLOCK(true, false),
    ARRAY_BLOCK(true, false),
    PROPERTY_KEY_TOKEN_NAME(false, true),
    RELATIONSHIP_TYPE_TOKEN_NAME(false, true),
    LABEL_TOKEN_NAME(false, true),
    NODE_LABELS(false, false),
    RELATIONSHIP_GROUP(true, false);

    private final boolean highActivity;
    private final boolean schemaType;

    RecordIdType(boolean highActivity, boolean schemaType) {
        this.highActivity = highActivity;
        this.schemaType = schemaType;
    }

    @Override
    public boolean highActivity() {
        return highActivity;
    }

    @Override
    public boolean isSchemaType() {
        return schemaType;
    }
}
