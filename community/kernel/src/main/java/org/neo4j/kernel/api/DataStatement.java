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

import java.util.Iterator;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class DataStatement extends BaseStatement implements DataRead, DataWrite
{
    DataStatement( KernelTransactionImplementation transaction, Statement statement )
    {
        super( transaction, statement );
    }

    // <DataRead>
    @Override
    public PrimitiveLongIterator nodesGetForLabel( long labelId )
    {
        assertOpen();
        return dataRead().nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        assertOpen();
        return dataRead().nodesGetFromIndexLookup( state, index, value );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeHasLabel( state, nodeId, labelId );
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeGetLabels( state, nodeId );
    }

    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeGetProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataRead().relationshipGetProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphGetProperty( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        assertOpen();
        return dataRead().graphGetProperty( state, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeHasProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataRead().relationshipHasProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public boolean graphHasProperty( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        assertOpen();
        return dataRead().graphHasProperty( state, propertyKeyId );
    }

    @Override
    public Iterator<SafeProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeGetAllProperties( state, nodeId );
    }

    @Override
    public Iterator<SafeProperty> relationshipGetAllProperties( long relationshipId )
            throws EntityNotFoundException
    {
        assertOpen();
        return dataRead().relationshipGetAllProperties( state, relationshipId );
    }

    @Override
    public Iterator<SafeProperty> graphGetAllProperties()
    {
        assertOpen();
        return dataRead().graphGetAllProperties( state );
    }
    // </DataRead>

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
    public Property nodeSetProperty( long nodeId, SafeProperty property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException, ConstraintValidationKernelException
    {
        assertOpen();
        return dataWrite().nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, SafeProperty property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataWrite().relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( SafeProperty property ) throws PropertyKeyIdNotFoundException
    {
        assertOpen();
        return dataWrite().graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataWrite().nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        assertOpen();
        return dataWrite().relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        assertOpen();
        return dataWrite().graphRemoveProperty( state, propertyKeyId );
    }
    // </DataWrite>
}
