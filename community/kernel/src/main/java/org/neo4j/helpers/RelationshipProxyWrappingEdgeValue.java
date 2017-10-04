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
package org.neo4j.helpers;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

public class RelationshipProxyWrappingEdgeValue extends EdgeValue
{
    private final Relationship relationship;
    private volatile TextValue type;
    private volatile MapValue properties;
    private volatile NodeValue startNode;
    private volatile NodeValue endNode;

    RelationshipProxyWrappingEdgeValue( Relationship relationship )
    {
        super( relationship.getId() );
        this.relationship = relationship;
    }

    public Relationship relationshipProxy()
    {
        return relationship;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        MapValue p;
        try
        {
            p = properties();
        }
        catch ( NotFoundException e )
        {
            p = VirtualValues.EMPTY_MAP;

        }

        if ( id() < 0 )
        {
            writer.writeVirtualEdgeHack( relationship );
        }

        writer.writeEdge( id(), startNode().id(), endNode().id(), type(), p );
    }

    @Override
    public NodeValue startNode()
    {
        NodeValue start = startNode;
        if ( start == null )
        {
            synchronized ( this )
            {
                start = startNode;
                if ( start == null )
                {
                    start = startNode = ValueUtils.fromNodeProxy( relationship.getStartNode() );
                }
            }
        }
        return start;
    }

    @Override
    public NodeValue endNode()
    {
        NodeValue end = endNode;
        if ( end == null )
        {
            synchronized ( this )
            {
                end = endNode;
                if ( end == null )
                {
                    end = endNode = ValueUtils.fromNodeProxy( relationship.getEndNode() );
                }
            }
        }
        return end;
    }

    @Override
    public TextValue type()
    {
        TextValue t = type;
        if ( t == null )
        {
            synchronized ( this )
            {
                t = type;
                if ( t == null )
                {
                    t = type = Values.stringValue( relationship.getType().name() );
                }
            }
        }
        return t;
    }

    @Override
    public MapValue properties()
    {
        MapValue m = properties;
        if ( m == null )
        {
            synchronized ( this )
            {
                m = properties;
                if ( m == null )
                {
                    m = properties = ValueUtils.asMapValue( relationship.getAllProperties() );
                }
            }
        }
        return m;
    }
}

