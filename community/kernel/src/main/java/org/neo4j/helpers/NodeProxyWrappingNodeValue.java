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
package org.neo4j.cypher.internal.javacompat;

import java.util.ArrayList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

public class NodeProxyWrappingNodeValue extends NodeValue
{
    private final Node node;
    private volatile TextArray labels;
    private volatile MapValue properties;

    NodeProxyWrappingNodeValue( Node node )
    {
        super( node.getId() );
        this.node = node;
    }

    public Node nodeProxy()
    {
        return node;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        TextArray l;
        MapValue p;
        try
        {
            l = labels();
            p = properties();
        }
        catch ( NotFoundException e )
        {
            l = Values.stringArray();
            p = VirtualValues.EMPTY_MAP;

        }

        if ( id() < 0 )
        {
            writer.writeVirtualNodeHack( node );
        }

        writer.writeNode( node.getId(), l, p );
    }

    @Override
    public TextArray labels()
    {
        TextArray l = labels;
        if ( l == null )
        {
            synchronized ( this )
            {
                l = labels;
                if ( l == null )
                {
                    ArrayList<String> ls = new ArrayList<>();
                    for ( Label label : node.getLabels() )
                    {
                        ls.add( label.name() );
                    }
                    l = labels = Values.stringArray( ls.toArray( new String[ls.size()] ) );

                }
            }
        }
        return l;
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
                    m = properties = ValueUtils.asMapValue( node.getAllProperties() );
                }
            }
        }
        return m;
    }
}
