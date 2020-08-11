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
package org.neo4j.internal.schema;

/**
 * Describe the capability of an index to return the property values for a given query in order.
 */
public enum IndexOrderCapability
{
    BOTH_FULLY_SORTED( true, true, true ),
    BOTH_PARTIALLY_SORTED( true, true, false ),
    ASC_FULLY_SORTED( true, false, true ),
    ASC_PARTIALLY_SORTED( true, false, false ),
    DESC_FULLY_SORTED( false, true, true ),
    DESC_PARTIALLY_SORTED( false, true, false ),
    NONE( false, false, false );

    private final boolean asc;
    private final boolean desc;
    private final boolean fullySorted;

    IndexOrderCapability( boolean asc, boolean desc, boolean fullySorted )
    {
        this.asc = asc;
        this.desc = desc;
        this.fullySorted = fullySorted;
    }

    /**
     * Does the index support returning results in ascending order?
     */
    public boolean supportsAsc()
    {
        return this.asc;
    }

    /**
     * Does the index support returning results in descending order?
     */
    public boolean supportsDesc()
    {
        return this.desc;
    }

    /**
     * @return if {@code true}, all elements returned from the index will be in the correct order,
     * if {@code false}, all elements except geometries returned from the index will be in the correct order.
     */
    public boolean isFullySorted()
    {
        return this.fullySorted;
    }
}
