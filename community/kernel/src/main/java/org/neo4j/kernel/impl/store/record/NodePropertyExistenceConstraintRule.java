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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;

public class NodePropertyExistenceConstraintRule extends NodePropertyConstraintRule
{
    private final int propertyKeyId;

    public static NodePropertyExistenceConstraintRule nodePropertyExistenceConstraintRule( long id, int labelId,
            int propertyKeyId )
    {
        return new NodePropertyExistenceConstraintRule( id, labelId, propertyKeyId );
    }

    public static NodePropertyExistenceConstraintRule readNodePropertyExistenceConstraintRule( long id, int labelId,
            ByteBuffer buffer )
    {
        return new NodePropertyExistenceConstraintRule( id, labelId, readPropertyKey( buffer ) );
    }

    private NodePropertyExistenceConstraintRule( long id, int labelId, int propertyKeyId )
    {
        super( id, labelId, Kind.NODE_PROPERTY_EXISTENCE_CONSTRAINT );
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public String toString()
    {
        return "NodePropertyExistenceConstraintRule[id=" + id + ", label=" + label + ", kind=" + kind +
               ", propertyKeyId=" + propertyKeyId + "]";
    }

    @Override
    public int length()
    {
        return 4 /* label id */ +
               1 /* kind id */ +
               4; /* property key id */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( label );
        target.put( kind.id() );
        target.putInt( propertyKeyId );
    }

    private static int readPropertyKey( ByteBuffer buffer )
    {
        return buffer.getInt();
    }

    public int getPropertyKey()
    {
        return propertyKeyId;
    }

    @Override
    public NodePropertyConstraint toConstraint()
    {
        return new NodePropertyExistenceConstraint( getLabel(), getPropertyKey() );
    }

    @Override
    public boolean containsPropertyKeyId( int propertyKeyId )
    {
        return propertyKeyId == this.propertyKeyId;
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
        if ( !super.equals( o ) )
        {
            return false;
        }
        return propertyKeyId == ((NodePropertyExistenceConstraintRule) o).propertyKeyId;

    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + propertyKeyId;
    }
}
