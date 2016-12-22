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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

public class NodePropertyExistenceConstraintViolationKernelException extends ConstraintViolationKernelException
{
    private final NodePropertyDescriptor descriptor;
    private final long nodeId;

    public NodePropertyExistenceConstraintViolationKernelException( NodePropertyDescriptor descriptor, long nodeId )
    {
        super( "Node %d with label %d must have the property %d",
                nodeId, descriptor.getEntityId(), descriptor.getPropertyKeyId() );
        this.descriptor = descriptor;
        this.nodeId = nodeId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "Node %d with label \"%s\" must have the property \"%s\" due to a constraint", nodeId,
                descriptor.entityNameText( tokenNameLookup ), descriptor.propertyNameText( tokenNameLookup ) );
    }
}
