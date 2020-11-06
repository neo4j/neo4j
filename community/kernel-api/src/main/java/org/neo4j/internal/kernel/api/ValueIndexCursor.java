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
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.Value;

public interface ValueIndexCursor
{
    /**
     * @return the number of properties accessible within the index, and thus from this cursor.
     */
    int numberOfProperties();

    int propertyKey( int offset );

    /**
     * Check before trying to access values with {@link #propertyValue(int)}. Result can change with each call to {@link #next()}.
     *
     * @return {@code true} if {@link #propertyValue(int)} can be used to get property value on cursor's current location,
     * else {@code false}.
     */
    boolean hasValue();

    Value propertyValue( int offset );
}
