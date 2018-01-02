/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.messaging.example;

import java.util.Arrays;
import java.util.List;

import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualValues;

public class Support
{
    static final TextArray NO_LABELS = Values.stringArray();
    static final MapValue NO_PROPERTIES = VirtualValues.EMPTY_MAP;

    private Support()
    {
    }

    // Helper to produce literal list of nodes
    public static NodeValue[] nodes( NodeValue... nodes )
    {
        return nodes;
    }

    // Helper to extract list of nodes from a path
    public static List<NodeValue> nodes( PathValue path )
    {
        return Arrays.asList( path.nodes() );
    }

    // Helper to produce literal list of relationships
    public static EdgeValue[] edges( EdgeValue... edgeValues )
    {
        return edgeValues;
    }
}
