/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;

public class EdgeValue extends VirtualEdgeValue
{
    private final long id;
    private final NodeValue startNode;
    private final NodeValue endNode;
    private final TextValue type;
    private final MapValue properties;

    EdgeValue( long id, NodeValue startNode, NodeValue endNode, TextValue type, MapValue properties )
    {
        assert properties != null;

        this.startNode = startNode;
        this.endNode = endNode;
        this.id = id;
        this.type = type;
        this.properties = properties;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.writeEdge( id, startNode.id(), endNode.id(), type, properties );
    }

    @Override
    public String toString()
    {
        return format( "-[%d]-", id );
    }

    public NodeValue startNode()
    {
        return startNode;
    }

    public NodeValue endNode()
    {
        return endNode;
    }

    @Override
    public long id()
    {
        return id;
    }

    public TextValue type()
    {
        return type;
    }

    public MapValue properties()
    {
        return properties;
    }

    public NodeValue otherNode( NodeValue node )
    {
        return node.equals( startNode ) ? endNode : startNode;
    }
}
