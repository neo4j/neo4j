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
package org.neo4j.procedure.builtin.routing;

/**
 * Defines a capability of an endpoint.
 * <p>
 * Note that a particular server might have several capabilities
 * but they will all be reported as distinct endpoints.
 */
public enum Role
{
    /**
     * Supports read operations.
     */
    READ,

    /**
     * Supports write operations.
     */
    WRITE,

    /**
     * Supports the GetServers procedure and is thus
     * capable of participating in discovery and load
     * balancing.
     */
    ROUTE
}
