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
package org.neo4j.internal.kernel.api;

/**
 * Describe the capability of an index to also return the exact property value for a given query.
 */
public enum IndexValueCapability
{
    YES( 3 ),     // Can provide values for query
    PARTIAL( 2 ), // Can provide values for query for part of result set, often depending on what type the value has
    NO( 1 );      // Can not provide values for query

    /**
     * Higher order indicate a higher capability.
     */
    private final int order;

    IndexValueCapability( int order )
    {
        this.order = order;
    }

    /**
     * Positive result if this capability is higher than other.
     * Negative result if this capability is lower that other.
     * Zero if this has same capability as other.
     */
    public int compare( IndexValueCapability other )
    {
        return Integer.compare( order, other.order );
    }
}
