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
package org.neo4j.storageengine.api.cursor;

public class CursorTypes
{
    public static final short NODE_CURSOR = 0;
    public static final short GROUP_CURSOR = 1;
    public static final short SCHEMA_CURSOR = 2;
    public static final short RELATIONSHIP_CURSOR = 3;
    public static final short PROPERTY_CURSOR = 4;
    public static final short DYNAMIC_ARRAY_STORE_CURSOR = 5;
    public static final short DYNAMIC_STRING_STORE_CURSOR = 6;
    public static final short DYNAMIC_LABEL_STORE_CURSOR = 7;
    public static final short DYNAMIC_REL_TYPE_TOKEN_CURSOR = 8;
    public static final short REL_TYPE_TOKEN_CURSOR = 9;
    public static final short DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR = 10;
    public static final short PROPERTY_KEY_TOKEN_CURSOR = 11;
    public static final short DYNAMIC_LABEL_TOKEN_CURSOR = 12;
    public static final short LABEL_TOKEN_CURSOR = 13;

    public static final short MAX_TYPE = LABEL_TOKEN_CURSOR;
}
