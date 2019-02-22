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
package org.neo4j.kernel.builtinprocs.routing;

/**
 * Enumerates the parameter names used for the GetServers
 * procedures in a causal cluster.
 */
public enum ParameterNames
{
    /**
     * Type: IN
     *
     * An opaque key-value map for supplying client context.
     *
     * Refer to the specific routing plugin deployed to
     * understand which specific keys can be utilised.
     */
    CONTEXT( "context" ),

    /**
     * Type: OUT
     *
     * Contains a map of endpoints and their associated capability.
     *
     * Refer to the protocol specification to understand the
     * exact format and how to utilise it.
     */
    SERVERS( "servers" ),

    /**
     * Type: OUT
     *
     * Defines the time-to-live of the returned information,
     * after which it shall be refreshed.
     *
     * Refer to the specific routing plugin deployed to
     * understand the impact of this setting.
     */
    TTL( "ttl" );

    private final String parameterName;

    ParameterNames( String parameterName )
    {
        this.parameterName = parameterName;
    }

    public String parameterName()
    {
        return parameterName;
    }
}
