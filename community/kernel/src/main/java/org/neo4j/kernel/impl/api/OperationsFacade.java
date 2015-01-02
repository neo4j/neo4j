/**
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

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

public class OperationsFacade implements ReadOperations, DataWriteOperations, SchemaWriteOperations
{
    final KernelStatement statement;
    private final LegacyKernelOperations legacyKernelOperations;
    private final StatementOperationParts operations;

    OperationsFacade( KernelStatement statement, LegacyKernelOperations legacyKernelOperations,
                      StatementOperationParts operations )
    {
        this.statement = statement;
        this.legacyKernelOperations = legacyKernelOperations;
        this.operations = operations;
    }

    // <DataRead>
    final KeyReadOperations tokenRead()
    {
        return operations.keyReadOperations();
    }

    final KeyWriteOperations tokenWrite()
    {
        return operations.keyWriteOperations();
    }

    final EntityReadOperations dataRead()
    {
        return operations.entityReadOperations();
    }

    final EntityWriteOperations dataWrite()
    {
        return operations.entityWriteOperations();
    }

    final SchemaReadOperations schemaRead()
    {
        return operations.schemaReadOperations();
    }

    final org.neo4j.kernel.impl.api.operations.SchemaWriteOperations schemaWrite()
    {
        return operations.schemaWriteOperations();
    }

    final SchemaStateOperations schemaState()
    {
        return operations.schemaStateOperations();
    }

    final LegacyKernelOperations legacyOps()
    {
        return legacyKernelOperations;
    }

    // <DataRead>
    @Override
    public PrimitiveLongIterator nodesGetForLabel( int labelId )
    {
        statement.assertOpen();
        if ( labelId == StatementConstants.NO_SUCH_LABEL )
        {
            return emptyPrimitiveLongIterator();
        }
        return dataRead().nodesGetForLabel( statement, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexLookup( statement, index, value );
    }

    @Override
    public long nodeGetUniqueFromIndexLookup( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        statement.assertOpen();
        return dataRead().nodeGetUniqueFromIndexLookup( statement, index, value );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return labelId != StatementConstants.NO_SUCH_LABEL && dataRead().nodeHasLabel( statement, nodeId, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().nodeGetLabels( statement, nodeId );
    }

    @Override
    public Property nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Property.noNodeProperty( nodeId, propertyKeyId );
        }
        return dataRead().nodeGetProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Property.noRelationshipProperty( relationshipId, propertyKeyId );
        }
        return dataRead().relationshipGetProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphGetProperty( int propertyKeyId )
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Property.noGraphProperty( propertyKeyId );
        }
        return dataRead().graphGetProperty( statement, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().nodeGetAllProperties( statement, nodeId );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( long relationshipId )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().relationshipGetAllProperties( statement, relationshipId );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties()
    {
        statement.assertOpen();
        return dataRead().graphGetAllProperties( statement );
    }
    // </DataRead>

    // <SchemaRead>
    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().indexesGetAll( statement );
    }

    @Override
    public IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException

    {
        IndexDescriptor result = null;
        Iterator<IndexDescriptor> indexes = uniqueIndexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            if ( index.getPropertyKeyId() == propertyKeyId )
            {
                if ( null == result )
                {
                    result = index;
                }
                else
                {
                    throw new SchemaRuleNotFoundException( labelId, propertyKeyId, "duplicate uniqueness index" );
                }
            }
        }

        if ( null == result )
        {
            throw new SchemaRuleNotFoundException( labelId, propertyKeyId, "uniqueness index not found" );
        }

        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( statement, labelId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexGetOwningUniquenessConstraintId( statement, index );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetAll( statement );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetState( statement, descriptor );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetFailure( statement, descriptor );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        statement.assertOpen();
        return schemaRead().constraintsGetAll( statement );
    }
    // </SchemaRead>

    // <TokenRead>
    @Override
    public int labelGetForName( String labelName )
    {
        statement.assertOpen();
        return tokenRead().labelGetForName( statement, labelName );
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().labelGetName( statement, labelId );
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetForName( statement, propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetName( statement, propertyKeyId );
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetAllTokens( statement );
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().labelsGetAllTokens( statement );
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetForName( statement, relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetName( statement, relationshipTypeId );
    }
    // </TokenRead>

    // <TokenWrite>
    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        statement.assertOpen();
        return tokenWrite().labelGetOrCreateForName( statement, labelName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        return tokenWrite().propertyKeyGetOrCreateForName( statement, propertyKeyName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        return tokenWrite().relationshipTypeGetOrCreateForName( statement, relationshipTypeName );
    }
    // </TokenWrite>

    // <SchemaState>
    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        return schemaState().schemaStateGetOrCreate( statement, key, creator );
    }
    // </SchemaState>

    // <DataWrite>
    @Override
    public long nodeCreate()
    {
        statement.assertOpen();
        return legacyOps().nodeCreate( statement );
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        statement.assertOpen();
        dataWrite().nodeDelete( statement, nodeId );
    }

    @Override
    public long relationshipCreate( long relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        return legacyOps().relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( long relationshipId )
    {
        statement.assertOpen();
        dataWrite().relationshipDelete( statement, relationshipId );
    }

    @Override
    public boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();
        return dataWrite().nodeAddLabel( statement, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveLabel( statement, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();
        return dataWrite().nodeSetProperty( statement, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().relationshipSetProperty( statement, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( DefinedProperty property )
    {
        statement.assertOpen();
        return dataWrite().graphSetProperty( statement, property );
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().relationshipRemoveProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( int propertyKeyId )
    {
        statement.assertOpen();
        return dataWrite().graphRemoveProperty( statement, propertyKeyId );
    }
    // </DataWrite>

    // <SchemaWrite>
    @Override
    public IndexDescriptor indexCreate( int labelId, int propertyKeyId )
            throws AddIndexFailureException, AlreadyIndexedException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().indexCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().indexDrop( statement, descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        statement.assertOpen();
        return schemaWrite().uniquenessConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWrite().uniqueIndexDrop( statement, descriptor );
    }
    // </SchemaWrite>
}
