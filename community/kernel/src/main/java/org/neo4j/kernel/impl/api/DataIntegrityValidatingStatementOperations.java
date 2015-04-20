/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException.OperationContext;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class DataIntegrityValidatingStatementOperations implements
    KeyWriteOperations,
    SchemaWriteOperations
{
    private final KeyWriteOperations keyWriteDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final SchemaWriteOperations schemaWriteDelegate;

    public DataIntegrityValidatingStatementOperations(
            KeyWriteOperations keyWriteDelegate,
            SchemaReadOperations schemaReadDelegate,
            SchemaWriteOperations schemaWriteDelegate )
    {
        this.keyWriteDelegate = keyWriteDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.schemaWriteDelegate = schemaWriteDelegate;
    }

    @Override
    public int propertyKeyGetOrCreateForName( Statement state, String propertyKey )
            throws IllegalTokenNameException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        return keyWriteDelegate.propertyKeyGetOrCreateForName( state, checkValidTokenName( propertyKey ) );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( Statement state, String relationshipTypeName )
            throws IllegalTokenNameException
    {
        return keyWriteDelegate.relationshipTypeGetOrCreateForName( state, checkValidTokenName( relationshipTypeName ) );
    }

    @Override
    public int labelGetOrCreateForName( Statement state, String label )
            throws IllegalTokenNameException, TooManyLabelsException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        return keyWriteDelegate.labelGetOrCreateForName( state, checkValidTokenName( label ) );
    }

    @Override
    public void labelCreateForName( KernelStatement state, String labelName, int id ) throws IllegalTokenNameException, TooManyLabelsException
    {
        keyWriteDelegate.labelCreateForName( state, labelName, id );
    }

    @Override
    public void propertyKeyCreateForName( KernelStatement state, String propertyKeyName, int id ) throws IllegalTokenNameException
    {
        keyWriteDelegate.propertyKeyCreateForName( state, propertyKeyName, id );
    }

    @Override
    public void relationshipTypeCreateForName( KernelStatement state, String relationshipTypeName, int id ) throws IllegalTokenNameException
    {
        keyWriteDelegate.relationshipTypeCreateForName( state, relationshipTypeName, id );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, int labelId, int propertyKey )
            throws AddIndexFailureException, AlreadyIndexedException, AlreadyConstrainedException
    {
        checkIndexExistence( state, OperationContext.INDEX_CREATION, labelId, propertyKey );
        return schemaWriteDelegate.indexCreate( state, labelId, propertyKey );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        try
        {
            assertIsNotUniqueIndex( descriptor, schemaReadDelegate.uniqueIndexesGetForLabel(
                    state, descriptor.getLabelId() ) );
            assertIndexExists( descriptor, schemaReadDelegate.indexesGetForLabel( state, descriptor.getLabelId() ) );
        }
        catch ( IndexBelongsToConstraintException | NoSuchIndexException e )
        {
            throw new DropIndexFailureException( descriptor, e );
        }
        schemaWriteDelegate.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteDelegate.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( KernelStatement state, int labelId, int propertyKey )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException
    {
        Iterator<UniquenessConstraint> constraints = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, labelId, propertyKey );
        if ( constraints.hasNext() )
        {
            throw new AlreadyConstrainedException( constraints.next(), OperationContext.CONSTRAINT_CREATION );
        }

        // It is not allowed to create uniqueness constraints on indexed label/property pairs
        checkIndexExistence( state, OperationContext.CONSTRAINT_CREATION, labelId, propertyKey );

        return schemaWriteDelegate.uniquenessConstraintCreate( state, labelId, propertyKey );
    }

    @Override
    public void constraintDrop( KernelStatement state, UniquenessConstraint constraint ) throws DropConstraintFailureException
    {
        try
        {
            assertConstraintExists( constraint, schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                    state, constraint.label(), constraint.propertyKeyId() ) );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( constraint, e );
        }
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    private void checkIndexExistence( KernelStatement state, OperationContext context, int labelId, int propertyKey )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        for ( IndexDescriptor descriptor : loop( schemaReadDelegate.indexesGetForLabel( state, labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new AlreadyIndexedException( descriptor, context );
            }
        }
        for ( IndexDescriptor descriptor : loop( schemaReadDelegate.uniqueIndexesGetForLabel( state, labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new AlreadyConstrainedException(
                        new UniquenessConstraint( descriptor.getLabelId(), descriptor.getPropertyKeyId() ), context );
            }
        }
    }

    private String checkValidTokenName( String name ) throws IllegalTokenNameException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new IllegalTokenNameException( name );
        }
        return name;
    }

    private void assertIsNotUniqueIndex( IndexDescriptor descriptor, Iterator<IndexDescriptor> uniqueIndexes )
            throws IndexBelongsToConstraintException

    {
        while ( uniqueIndexes.hasNext() )
        {
            IndexDescriptor uniqueIndex = uniqueIndexes.next();
            if ( uniqueIndex.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                throw new IndexBelongsToConstraintException( descriptor );
            }
        }
    }

    private void assertIndexExists( IndexDescriptor descriptor, Iterator<IndexDescriptor> indexes )
            throws NoSuchIndexException
    {
        for ( IndexDescriptor existing : loop( indexes ) )
        {
            if ( existing.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                return;
            }
        }
        throw new NoSuchIndexException( descriptor );
    }

    private void assertConstraintExists( UniquenessConstraint constraint, Iterator<UniquenessConstraint> constraints )
            throws NoSuchConstraintException
    {
        for ( UniquenessConstraint existing : loop( constraints ) )
        {
            if ( existing.equals( constraint.label(), constraint.propertyKeyId() ) )
            {
                return;
            }
        }
        throw new NoSuchConstraintException( constraint );
    }
}
