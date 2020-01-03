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
package org.neo4j.kernel.impl.api.state;

import javax.annotation.Nonnull;

import org.neo4j.graphdb.Resource;
import org.neo4j.values.storable.Value;

/**
 * A collection of values that associates a special {@code long} reference to each added value.
 * Instances must be closed with {@link #close()} to release underlying resources.
 */
public interface ValuesContainer extends Resource
{
    /**
     * @param value value to add
     * @return a reference associated with the value, that can be passed to {@link #get(long)} and {@link #remove(long)}
     * @throws IllegalStateException if container is closed
     */
    long add( @Nonnull Value value );

    /**
     * @param ref a reference obtained from {@link #add(Value)}
     * @return a value associated with the reference
     * @throws IllegalStateException if container is closed
     * @throws IllegalArgumentException if reference is invalid or associated value was removed
     */
    @Nonnull
    Value get( long ref );

    /**
     * @param ref a reference obtained from {@link #add(Value)}
     * @return a value associated with the reference
     * @throws IllegalStateException if container is closed
     * @throws IllegalArgumentException if reference is invalid or associated value was removed
     */
    @Nonnull
    Value remove( long ref );
}
