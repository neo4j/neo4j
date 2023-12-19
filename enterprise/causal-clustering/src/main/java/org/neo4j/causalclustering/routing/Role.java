/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing;

/**
 * Defines a capability of an endpoint.
 *
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
