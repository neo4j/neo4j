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

import java.util.Arrays;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBelongsToConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchIndexException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException.OperationContext;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

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
    public IndexDescriptor indexCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException
    {
        assertValidDescriptor( descriptor, OperationContext.INDEX_CREATION );
        assertIndexDoesNotExist( state, OperationContext.INDEX_CREATION, descriptor );
        return schemaWriteDelegate.indexCreate( state, descriptor );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor index ) throws DropIndexFailureException
    {
        try
        {
            IndexDescriptor existingIndex =
                    schemaReadDelegate.indexGetForSchema( state, index.schema() );

            if ( existingIndex == null )
            {
                throw new NoSuchIndexException( index.schema() );
            }

            if ( existingIndex.type() == UNIQUE )
            {
                throw new IndexBelongsToConstraintException( index.schema() );
            }
        }
        catch ( IndexBelongsToConstraintException | NoSuchIndexException e )
        {
            throw new DropIndexFailureException( index.schema(), e );
        }
        schemaWriteDelegate.indexDrop( state, index );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor index ) throws DropIndexFailureException
    {
        schemaWriteDelegate.uniqueIndexDrop( state, index );
    }

    @Override
    public NodeKeyConstraintDescriptor nodeKeyConstraintCreate(
            KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        assertValidDescriptor( descriptor, OperationContext.CONSTRAINT_CREATION );
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.nodeKeyForSchema( descriptor );
        assertConstraintDoesNotExist( state, constraint );

        // It is not allowed to create node key constraints on indexed label/property pairs
        assertIndexDoesNotExist( state, OperationContext.CONSTRAINT_CREATION, descriptor );

        return schemaWriteDelegate.nodeKeyConstraintCreate( state, descriptor );
    }

    @Override
    public UniquenessConstraintDescriptor uniquePropertyConstraintCreate(
            KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        assertValidDescriptor( descriptor, OperationContext.CONSTRAINT_CREATION );
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        assertConstraintDoesNotExist( state, constraint );

        // It is not allowed to create uniqueness constraints on indexed label/property pairs
        assertIndexDoesNotExist( state, OperationContext.CONSTRAINT_CREATION, descriptor );

        return schemaWriteDelegate.uniquePropertyConstraintCreate( state, descriptor );
    }

    @Override
    public NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate( KernelStatement state,
            LabelSchemaDescriptor descriptor) throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException
    {
        assertValidDescriptor( descriptor, OperationContext.CONSTRAINT_CREATION );
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor );
        assertConstraintDoesNotExist( state, constraint );
        return schemaWriteDelegate.nodePropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            RelationTypeSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException
    {
        assertValidDescriptor( descriptor, OperationContext.CONSTRAINT_CREATION );
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForSchema( descriptor );
        assertConstraintDoesNotExist( state, constraint );
        return schemaWriteDelegate.relationshipPropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public void constraintDrop( KernelStatement state, ConstraintDescriptor descriptor ) throws DropConstraintFailureException
    {
        try
        {
            assertConstraintExists( state, descriptor );
        }
        catch ( NoSuchConstraintException e )
        {
            throw new DropConstraintFailureException( descriptor , e );
        }
        schemaWriteDelegate.constraintDrop( state, descriptor );
    }

    private void assertIndexDoesNotExist( KernelStatement state, OperationContext context,
            LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        IndexDescriptor existingIndex = schemaReadDelegate.indexGetForSchema( state, descriptor );
        if ( existingIndex != null )
        {
            if ( existingIndex.type() == UNIQUE )
            {
                throw new AlreadyConstrainedException( ConstraintDescriptorFactory.uniqueForSchema( descriptor ),
                        context, new StatementTokenNameLookup( state.readOperations() ) );
            }
            throw new AlreadyIndexedException( descriptor, context );
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

    private void assertConstraintDoesNotExist( KernelStatement state, ConstraintDescriptor constraint )
            throws AlreadyConstrainedException
    {
        if ( schemaReadDelegate.constraintExists( state, constraint ) )
        {
            throw new AlreadyConstrainedException( constraint, OperationContext.CONSTRAINT_CREATION,
                    new StatementTokenNameLookup( state.readOperations() ) );
        }
    }

    private void assertConstraintExists( KernelStatement state, ConstraintDescriptor constraint )
            throws NoSuchConstraintException
    {
        if ( !schemaReadDelegate.constraintExists( state, constraint ) )
        {
            throw new NoSuchConstraintException( constraint );
        }
    }

    private void assertValidDescriptor( SchemaDescriptor descriptor, OperationContext context )
            throws RepeatedPropertyInCompositeSchemaException
    {
        int numUnique = Arrays.stream( descriptor.getPropertyIds() ).distinct().toArray().length;
        if ( numUnique != descriptor.getPropertyIds().length )
        {
            throw new RepeatedPropertyInCompositeSchemaException( descriptor, context );
        }
    }
}
