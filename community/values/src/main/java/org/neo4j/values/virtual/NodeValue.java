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

import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

public class NodeValue extends VirtualValue
{
    private final long id;
    private final LabelSet labels;
    private final MapValue properties;

    public NodeValue( long id, LabelSet labels, MapValue properties )
    {
        assert labels != null;
        assert properties != null;

        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
       writer.writeNodeReference( id );
    }

    @Override
    public int hash()
    {
        return Long.hashCode( id ) + 31 * ( labels.hashCode() + 31 * properties.hashCode() );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof NodeValue) )
        {
            return false;
        }
        NodeValue that = (NodeValue) other;
        return id == that.id && labels.equals( that.labels ) && properties.equals( that.properties );
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.NODE;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof  NodeValue ) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        NodeValue otherNode = (NodeValue) other;
        return Long.compare( id, otherNode.id );
    }
}
