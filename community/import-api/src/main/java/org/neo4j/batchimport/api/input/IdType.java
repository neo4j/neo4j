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
package org.neo4j.batchimport.api.input;

/**
 * Defines different types that input ids can come in. Enum names in here are user facing.
 */
public enum IdType {
    /**
     * Used when node ids int input data are any string identifier.
     */
    STRING,

    /**
     * Used when node ids int input data are any integer identifier. It uses 8b longs for storage,
     * but as a user facing enum a better name is integer
     */
    INTEGER,

    /**
     * Used when node ids int input data are specified as long values and points to actual record ids.
     * ADVANCED usage. Performance advantage, but requires carefully planned input data.
     */
    ACTUAL
}
