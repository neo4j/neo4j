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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.values.storable.Value;

/**
 * A node together with properties. This class is needed to present changes in the transaction state to index operations
 * that require knowing the affected property values as well.
 */
public class NodeWithPropertyValues
{

    private final long nodeId;
    private final Value[] values;

    NodeWithPropertyValues( long nodeId, Value[] values )
    {
        this.nodeId = nodeId;
        this.values = values;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public Value[] getValues()
    {
        return values;
    }

    @Override
    public String toString()
    {
        return "NodeWithPropertyValues{" + "nodeId=" + nodeId + ", values=" + Arrays.toString( values ) + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        NodeWithPropertyValues that = (NodeWithPropertyValues) o;
        return nodeId == that.nodeId && Arrays.equals( values, that.values );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( nodeId );
        result = 31 * result + Arrays.hashCode( values );
        return result;
    }
}
