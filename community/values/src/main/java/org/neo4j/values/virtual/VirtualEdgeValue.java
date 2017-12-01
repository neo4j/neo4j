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
import org.neo4j.values.VirtualValue;

public abstract class VirtualEdgeValue extends VirtualValue
{
    public abstract long id();

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof VirtualEdgeValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }

        VirtualEdgeValue otherNode = (VirtualEdgeValue) other;
        return Long.compare( id(), otherNode.id() );
    }

    @Override
    public int computeHash()
    {
        return Long.hashCode( id() );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( !(other instanceof VirtualEdgeValue) )
        {
            return false;
        }
        VirtualEdgeValue that = (VirtualEdgeValue) other;
        return id() == that.id();
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.EDGE;
    }
}
