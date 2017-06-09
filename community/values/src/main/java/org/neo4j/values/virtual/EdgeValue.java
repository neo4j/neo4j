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
import org.neo4j.values.VirtualValue;

public class EdgeValue extends VirtualValue
{
    private final long id;
    private final long startNodeId;
    private final long endNodeId;
    private final int label;
    private final MapValue properties;

    public EdgeValue( long id, long startNodeId, long endNodeId, int label, MapValue properties )
    {
        assert properties != null;

        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public void writeTo( AnyValueWriter writer )
    {
        writer.beginEdge( id );
        writer.writeLabel( label );
        writer.beginProperties( properties.size() );
        for ( int i = 0; i < properties.size(); i++ )
        {
            writer.writePropertyKeyId( properties.propertyKeyId( i ) );
            properties.value( i ).writeTo( writer );
        }
        writer.endProperties();
        writer.endEdge();
    }

    @Override
    public int hash()
    {
        return Long.hashCode( id ) + 31 * ( label + 31 * properties.hashCode() );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof EdgeValue) )
        {
            return false;
        }
        EdgeValue that = (EdgeValue) other;
        return id == that.id && label == that.label && properties.equals( that.properties );
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.EDGE;
    }
}
