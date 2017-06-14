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
import org.neo4j.values.TextValue;
import org.neo4j.values.VirtualValue;

public class LabelValue extends VirtualValue
{
    private final int id;
    private final TextValue value;

    LabelValue( int id, TextValue value )
    {
        assert id >= 0;
        assert value != null;

        this.id = id;
        this.value  = value;
    }

    int id()
    {
        return id;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
       writer.writeNodeReference( id );
    }

    @Override
    public int hash()
    {
        return Integer.hashCode( id ) + 31 * ( value.hashCode() );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof LabelValue) )
        {
            return false;
        }
        LabelValue that = (LabelValue) other;
        return id == that.id && value.equals( that.value );
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.LABEL;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof LabelValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        LabelValue otherNode = (LabelValue) other;
        return Long.compare( id, otherNode.id );
    }
}
