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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
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
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

import static org.neo4j.helpers.collection.Iterators.loop;

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
    public IndexDescriptor indexCreate( KernelStatement state, NodePropertyDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        checkIndexExistence( state, OperationContext.INDEX_CREATION, descriptor );
        return schemaWriteDelegate.indexCreate( state, descriptor );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor index ) throws DropIndexFailureException
    {
        try
        {
            assertIsNotUniqueIndex( index, schemaReadDelegate.uniqueIndexesGetForLabel(
                    state, index.getLabelId() ) );
            assertIndexExists( index, schemaReadDelegate.indexesGetForLabel( state, index.getLabelId() ) );
        }
        catch ( IndexBelongsToConstraintException | NoSuchIndexException e )
        {
            throw new DropIndexFailureException( index.descriptor(), e );
        }
        schemaWriteDelegate.indexDrop( state, index );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor index ) throws DropIndexFailureException
    {
        schemaWriteDelegate.uniqueIndexDrop( state, index );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state,
            NodePropertyDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException
    {
        Iterator<NodePropertyConstraint> constraints = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, descriptor );
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            if ( constraint instanceof UniquenessConstraint )
            {
                throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
        }

        // It is not allowed to create uniqueness constraints on indexed label/property pairs
        checkIndexExistence( state, OperationContext.CONSTRAINT_CREATION, descriptor );

        return schemaWriteDelegate.uniquePropertyConstraintCreate( state, descriptor );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( KernelStatement state,
            NodePropertyDescriptor descriptor) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        Iterator<NodePropertyConstraint> constraints = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, descriptor );
        while ( constraints.hasNext() )
        {
            NodePropertyConstraint constraint = constraints.next();
            if ( constraint instanceof NodePropertyExistenceConstraint )
            {
                throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
        }

        return schemaWriteDelegate.nodePropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            RelationshipPropertyDescriptor descriptor ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        Iterator<RelationshipPropertyConstraint> constraints =
                schemaReadDelegate.constraintsGetForRelationshipTypeAndPropertyKey( state, descriptor );
        while ( constraints.hasNext() )
        {
            RelationshipPropertyConstraint constraint = constraints.next();
            if ( constraint instanceof RelationshipPropertyExistenceConstraint )
            {
                throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
        }

        return schemaWriteDelegate.relationshipPropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public void constraintDrop( KernelStatement state, NodePropertyConstraint constraint ) throws DropConstraintFailureException
    {
        try
        {
            assertConstraintExists( constraint,
                    schemaReadDelegate.constraintsGetForLabelAndPropertyKey( state, constraint.descriptor() ) );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( constraint, e );
        }
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public void constraintDrop( KernelStatement state, RelationshipPropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        try
        {
            assertConstraintExists( constraint, schemaReadDelegate.constraintsGetForRelationshipTypeAndPropertyKey(
                    state, constraint.descriptor() ) );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( constraint, e );
        }
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    private void checkIndexExistence( KernelStatement state, OperationContext context,
            NodePropertyDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        //TODO: Consider using schemaReadDelegate.indexGetForLabelAndPropertyKey(state, indexDescriptor)
        for ( IndexDescriptor index : loop( schemaReadDelegate.indexesGetForLabel( state, descriptor.getLabelId() ) ) )
        {
            if ( index.equals( descriptor ) )
            {
                throw new AlreadyIndexedException( index.descriptor(), context );
            }

        }
        for ( IndexDescriptor index : loop(
                schemaReadDelegate.uniqueIndexesGetForLabel( state, descriptor.getLabelId() ) ) )
        {
            if ( index.equals( descriptor ) )
            {
                throw new AlreadyConstrainedException(
                        new UniquenessConstraint( descriptor ), context,
                        new StatementTokenNameLookup( state.readOperations() ) );
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

    private void assertIsNotUniqueIndex( IndexDescriptor index, Iterator<IndexDescriptor> uniqueIndexes )
            throws IndexBelongsToConstraintException

    {
        while ( uniqueIndexes.hasNext() )
        {
            IndexDescriptor uniqueIndex = uniqueIndexes.next();
            if ( uniqueIndex.equals( index ) )
            {
                throw new IndexBelongsToConstraintException( index.descriptor() );
            }
        }
    }

    private void assertIndexExists( IndexDescriptor index, Iterator<IndexDescriptor> indexes )
            throws NoSuchIndexException
    {
        for ( IndexDescriptor existing : loop( indexes ) )
        {
            if ( existing.equals( index ) )
            {
                return;
            }
        }
        throw new NoSuchIndexException( index.descriptor() );
    }

    private <C extends PropertyConstraint> void assertConstraintExists( C constraint, Iterator<C> existingConstraints )
            throws NoSuchConstraintException
    {
        while ( existingConstraints.hasNext() )
        {
            C existing = existingConstraints.next();
            if ( existing.equals( constraint ) )
            {
                return;
            }
        }
        throw new NoSuchConstraintException( constraint );
    }
}
