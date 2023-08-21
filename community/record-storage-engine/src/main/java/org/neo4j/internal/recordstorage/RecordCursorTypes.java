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

import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;

import org.neo4j.storageengine.api.cursor.CursorType;

public enum RecordCursorTypes implements CursorType {
    NODE_CURSOR,
    GROUP_CURSOR,
    SCHEMA_CURSOR,
    RELATIONSHIP_CURSOR,
    PROPERTY_CURSOR,
    DYNAMIC_ARRAY_STORE_CURSOR,
    DYNAMIC_STRING_STORE_CURSOR,
    DYNAMIC_LABEL_STORE_CURSOR,
    DYNAMIC_REL_TYPE_TOKEN_CURSOR,
    REL_TYPE_TOKEN_CURSOR,
    DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR,
    PROPERTY_KEY_TOKEN_CURSOR,
    DYNAMIC_LABEL_TOKEN_CURSOR,
    LABEL_TOKEN_CURSOR;
    public static final short MAX_TYPE = LABEL_TOKEN_CURSOR.value();

    @Override
    public short value() {
        return safeCastIntToShort(ordinal());
    }
}
