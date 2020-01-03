/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * Cursor that can read property data.
 */
public interface StoragePropertyCursor extends StorageCursor
{
    /**
     * Initializes this cursor to that reading property data at the given {@code reference}.
     *
     * @param reference reference to start reading properties at.
     */
    void init( long reference );

    /**
     * @return property key of the property this cursor currently is placed at.
     */
    int propertyKey();

    /**
     * @return value group of the property this cursor currently is placed at.
     */
    ValueGroup propertyType();

    /**
     * @return value of the property this cursor currently is placed at.
     */
    Value propertyValue();
}
