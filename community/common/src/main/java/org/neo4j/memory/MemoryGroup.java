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
package org.neo4j.memory;

public enum MemoryGroup
{
    TRANSACTION( "Transaction", false ),
    NETTY( "Netty", true ),
    PAGE_CACHE( "Page Cache", true ),
    REPLICATION_BUFFERS( "Replication Buffers", true ),
    QUERY_CACHE( "Query Cache", false ),
    NO_TRACKING( "No Tracking", true );

    private final String name;
    private final boolean global;

    MemoryGroup( String name, boolean global )
    {
        this.name = name;
        this.global = global;
    }

    public String getName()
    {
        return name;
    }

    public boolean isGlobal()
    {
        return global;
    }
}
