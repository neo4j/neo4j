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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaAndDataModificationInSameTransactionException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.constraints.UnableToValidateConstraintKernelException;
import org.neo4j.kernel.impl.api.constraints.UniqueConstraintViolationKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class ConstraintEnforcingEntityWriteOperations implements EntityWriteOperations
{
    private final EntityWriteOperations entityWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final SchemaReadOperations schemaReadOperations;

    public ConstraintEnforcingEntityWriteOperations(
            EntityWriteOperations entityWriteOperations,
            EntityReadOperations entityReadOperations,
            SchemaReadOperations schemaReadOperations )
    {
        this.entityWriteOperations = entityWriteOperations;
        this.entityReadOperations = entityReadOperations;
        this.schemaReadOperations = schemaReadOperations;
    }

    @Override
    public boolean nodeAddLabel( StatementState state, long nodeId, long labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        Iterator<UniquenessConstraint> constraints = schemaReadOperations.constraintsGetForLabel( state, labelId );
        while ( constraints.hasNext() )
        {
            UniquenessConstraint constraint = constraints.next();
            long propertyKeyId = constraint.propertyKeyId();
            Property property;
            try
            {
                property = entityReadOperations.nodeGetProperty( state, nodeId, propertyKeyId );
            }
            catch ( PropertyKeyIdNotFoundException e )
            {
                throw new UnableToValidateConstraintKernelException( e );
            }
            if ( !property.isNoProperty() )
            {
                validateNoExistingNodeWithLabelAndProperty( state, labelId, (SafeProperty)property );
            }
        }
        return entityWriteOperations.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( StatementState state, long nodeId, SafeProperty property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException, ConstraintValidationKernelException
    {
        PrimitiveLongIterator labelIds = entityReadOperations.nodeGetLabels( state, nodeId );
        while ( labelIds.hasNext() )
        {
            long labelId = labelIds.next();
            long propertyKeyId = property.propertyKeyId();
            Iterator<UniquenessConstraint> constraintIterator =
                    schemaReadOperations.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
            if ( constraintIterator.hasNext() )
            {
                validateNoExistingNodeWithLabelAndProperty( state, labelId, property );
            }
        }
        return entityWriteOperations.nodeSetProperty( state, nodeId, property );
    }

    private void validateNoExistingNodeWithLabelAndProperty( StatementState state, long labelId, SafeProperty property )
            throws ConstraintValidationKernelException
    {
        try
        {
            Object value = property.value();
            IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, property.propertyKeyId() );
            verifyIndexOnline( state, indexDescriptor );
            state.locks().acquireIndexEntryWriteLock( labelId, property.propertyKeyId(), property.valueAsString() );
            PrimitiveLongIterator existingNodes = entityReadOperations.nodesGetFromIndexLookup(
                    state, indexDescriptor, value );
            if ( existingNodes.hasNext() )
            {

                throw new UniqueConstraintViolationKernelException( labelId, property.propertyKeyId(), value,
                                                                    existingNodes.next() );
            }
        }
        catch ( IndexNotFoundKernelException | SchemaAndDataModificationInSameTransactionException e )
        {
            throw new UnableToValidateConstraintKernelException( e );
        }
    }

    private void verifyIndexOnline( StatementState state, IndexDescriptor indexDescriptor )
            throws IndexNotFoundKernelException, SchemaAndDataModificationInSameTransactionException
    {
        if ( schemaReadOperations.indexGetState( state, indexDescriptor ) != InternalIndexState.ONLINE )
        {
            // The only case where the index would not be online is if the constraint was created in this transaction.
            throw new SchemaAndDataModificationInSameTransactionException();
        }
    }

    // Simply delegate the rest of the invocations

    @Override
    public void nodeDelete( StatementState state, long nodeId )
    {
        entityWriteOperations.nodeDelete( state, nodeId );
    }

    @Override
    public void relationshipDelete( StatementState state, long relationshipId )
    {
        entityWriteOperations.relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        return entityWriteOperations.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property relationshipSetProperty( StatementState state, long relationshipId, SafeProperty property ) throws
            PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return entityWriteOperations.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( StatementState state, SafeProperty property ) throws PropertyKeyIdNotFoundException
    {
        return entityWriteOperations.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return entityWriteOperations.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        return entityWriteOperations.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        return entityWriteOperations.graphRemoveProperty( state, propertyKeyId );
    }
}
