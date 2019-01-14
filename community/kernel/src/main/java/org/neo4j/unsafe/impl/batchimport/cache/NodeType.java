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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Convenient way of selecting nodes based on their dense status.
 */
public class NodeType
{
    public static final int NODE_TYPE_DENSE = 0x1;
    public static final int NODE_TYPE_SPARSE = 0x2;
    public static final int NODE_TYPE_ALL = NODE_TYPE_DENSE | NODE_TYPE_SPARSE;

    private NodeType()
    {
    }

    public static boolean isDense( int nodeTypes )
    {
        return has( nodeTypes, NODE_TYPE_DENSE );
    }

    public static boolean isSparse( int nodeTypes )
    {
        return has( nodeTypes, NODE_TYPE_SPARSE );
    }

    private static boolean has( int nodeTypes, int mask )
    {
        return (nodeTypes & mask) != 0;
    }

    public static boolean matchesDense( int nodeTypes, boolean isDense )
    {
        int mask = isDense ? NODE_TYPE_DENSE : NODE_TYPE_SPARSE;
        return has( nodeTypes, mask );
    }
}
