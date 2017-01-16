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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;

public class NodePropertyExistenceConstraintRule extends NodePropertyConstraintRule
{

    public static NodePropertyExistenceConstraintRule nodePropertyExistenceConstraintRule( long id, NodePropertyDescriptor descriptor )
    {
        return new NodePropertyExistenceConstraintRule( id, descriptor );
    }

    public static NodePropertyExistenceConstraintRule readNodePropertyExistenceConstraintRule( long id, int labelId,
            ByteBuffer buffer )
    {
        //TODO: Support composite constraints
        return new NodePropertyExistenceConstraintRule( id, new NodePropertyDescriptor( labelId, buffer.getInt() ) );
    }

    private NodePropertyExistenceConstraintRule( long id, NodePropertyDescriptor descriptor )
    {
        super( id, descriptor, Kind.NODE_PROPERTY_EXISTENCE_CONSTRAINT );
    }

    @Override
    public String toString()
    {
        return "NodePropertyExistenceConstraintRule[id=" + id + ", label=" + descriptor().getLabelId() + ", kind=" +
               kind + ", propertyKeyIds=" + descriptor.propertyIdText() + "]";
    }

    @Override
    public int length()
    {
        //TODO: Support composite indexes (requires format update)
        return 4 /* label id */ +
               1 /* kind id */ +
               4; /* property key id */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        //TODO: Support composite indexes (requires format update)
        target.putInt( descriptor().getLabelId() );
        target.put( kind.id() );
        target.putInt( descriptor.getPropertyKeyId() );
    }

    @Override
    public NodePropertyConstraint toConstraint()
    {
        return new NodePropertyExistenceConstraint( descriptor() );
    }
}
