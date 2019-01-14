/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index.collector;

/**
 * Represents a point-in-time view on a set of numeric values
 * that are read from a {@code NumericDocValues} field.
 */
public interface DocValuesAccess
{
    /**
     * @return the current value of the main field that is driving the values.
     */
    long current();

    /**
     * @return the value of an additional sidecar field.
     * @throws IllegalStateException if no such field is indexed.
     */
    long getValue( String field );
}
