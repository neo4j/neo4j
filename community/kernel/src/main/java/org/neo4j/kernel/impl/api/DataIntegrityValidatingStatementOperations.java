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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ProcedureException;
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
import org.neo4j.kernel.api.exceptions.schema.ProcedureConstraintViolation;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
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
            throws AlreadyIndexedException, AlreadyConstrainedException
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
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state, int labelId, int propertyKey )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException
    {
        Iterator<NodePropertyConstraint> constraints = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, labelId, propertyKey );
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
        checkIndexExistence( state, OperationContext.CONSTRAINT_CREATION, labelId, propertyKey );

        return schemaWriteDelegate.uniquePropertyConstraintCreate( state, labelId, propertyKey );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( KernelStatement state, int labelId,
            int propertyKey ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        Iterator<NodePropertyConstraint> constraints = schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                state, labelId, propertyKey );
        while ( constraints.hasNext() )
        {
            NodePropertyConstraint constraint = constraints.next();
            if ( constraint instanceof NodePropertyExistenceConstraint )
            {
                throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
        }

        return schemaWriteDelegate.nodePropertyExistenceConstraintCreate( state, labelId, propertyKey );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            int relTypeId, int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        Iterator<RelationshipPropertyConstraint> constraints = schemaReadDelegate.constraintsGetForRelationshipTypeAndPropertyKey(
                state, relTypeId, propertyKeyId );
        while ( constraints.hasNext() )
        {
            RelationshipPropertyConstraint constraint = constraints.next();
            if ( constraint instanceof RelationshipPropertyExistenceConstraint )
            {
                throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
        }

        return schemaWriteDelegate.relationshipPropertyExistenceConstraintCreate( state, relTypeId, propertyKeyId );
    }

    @Override
    public void constraintDrop( KernelStatement state, NodePropertyConstraint constraint ) throws DropConstraintFailureException
    {
        try
        {
            assertConstraintExists( constraint, schemaReadDelegate.constraintsGetForLabelAndPropertyKey(
                    state, constraint.label(), constraint.propertyKey() ) );
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
                    state, constraint.relationshipType(), constraint.propertyKey() ) );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( constraint, e );
        }
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public void procedureCreate( KernelStatement state, ProcedureSignature signature, String language, String code )
            throws ProcedureException, ProcedureConstraintViolation
    {
        if( schemaReadDelegate.procedureGet( state, signature.name() ) != null )
        {
            throw new ProcedureConstraintViolation("%s cannot be created because there is already a procedure with the same " +
                                                                  "name in the graph.",  signature.toString() );
        }
        schemaWriteDelegate.procedureCreate( state, signature, language, code );
    }

    @Override
    public void procedureDrop( KernelStatement statement, ProcedureSignature.ProcedureName name ) throws ProcedureException, ProcedureConstraintViolation
    {
        if( schemaReadDelegate.procedureGet( statement, name ) == null )
        {
            throw new ProcedureConstraintViolation("%s cannot be dropped because there is no such procedure in the graph.",  name );
        }
        schemaWriteDelegate.procedureDrop( statement, name );
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
                        new UniquenessConstraint( descriptor.getLabelId(), descriptor.getPropertyKeyId() ), context,
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
