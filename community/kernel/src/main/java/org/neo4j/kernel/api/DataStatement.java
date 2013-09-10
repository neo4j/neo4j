/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;

public class DataStatement extends ReadStatement implements DataWrite
{
    DataStatement( KernelTransactionImplementation transaction, Statement statement )
    {
        super( transaction, statement );
    }

    // <DataWrite>
    @Override
    public long nodeCreate()
    {
        assertOpen();
        return legacyOps().nodeCreate(state);
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        assertOpen();
        dataWrite().nodeDelete( state, nodeId );
    }

    @Override
    public long relationshipCreate( long relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        assertOpen();
        return legacyOps().relationshipCreate( state, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( long relationshipId )
    {
        assertOpen();
        dataWrite().relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        assertOpen();
        return dataWrite().nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataWrite().nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        assertOpen();
        return dataWrite().nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        assertOpen();
        return dataWrite().relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( DefinedProperty property )
    {
        assertOpen();
        return dataWrite().graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataWrite().nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataWrite().relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( long propertyKeyId )
    {
        assertOpen();
        return dataWrite().graphRemoveProperty( state, propertyKeyId );
    }
    // </DataWrite>
}
