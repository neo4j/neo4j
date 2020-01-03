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

/**
 * A {@link StorageCursor} for entities, i.e. which has properties.
 */
public interface StorageEntityCursor extends StorageCursor
{
    /**
     * @return {@code true} if the entity the cursor is at has any properties, otherwise {@code false}.
     */
    boolean hasProperties();

    /**
     * @return a {@code long} reference to start reading properties for the entity this cursor is at.
     */
    long propertiesReference();

    /**
     * @return reference to the entity this cursor currently is placed at.
     */
    long entityReference();
}
