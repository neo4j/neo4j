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

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

public class OperationsFacade implements ReadOperations, DataWriteOperations, SchemaWriteOperations
{
    private final KernelTransactionImplementation transaction;
    final Statement statement;

    OperationsFacade( KernelTransactionImplementation transaction, Statement statement )
    {
        this.transaction = transaction;
        this.statement = statement;
    }

    // <DataRead>
    final KeyReadOperations tokenRead()
    {
        return transaction.operations.keyReadOperations();
    }

    final KeyWriteOperations tokenWrite()
    {
        return transaction.operations.keyWriteOperations();
    }

    final EntityReadOperations dataRead()
    {
        return transaction.operations.entityReadOperations();
    }

    final EntityWriteOperations dataWrite()
    {
        return transaction.operations.entityWriteOperations();
    }

    final SchemaReadOperations schemaRead()
    {
        return transaction.operations.schemaReadOperations();
    }

    final org.neo4j.kernel.api.operations.SchemaWriteOperations schemaWrite()
    {
        return transaction.operations.schemaWriteOperations();
    }

    final SchemaStateOperations schemaState()
    {
        return transaction.operations.schemaStateOperations();
    }

    final LegacyKernelOperations legacyOps()
    {
        return transaction.legacyKernelOperations;
    }

    // <DataRead>
    @Override
    public PrimitiveLongIterator nodesGetForLabel( long labelId )
    {
        statement.assertOpen();
        if ( labelId == NO_SUCH_LABEL )
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
    public boolean nodeHasLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return labelId != NO_SUCH_LABEL && dataRead().nodeHasLabel( statement, nodeId, labelId );
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().nodeGetLabels( statement, nodeId );
    }

    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noNodeProperty( nodeId, propertyKeyId );
        }
        return dataRead().nodeGetProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noRelationshipProperty( relationshipId, propertyKeyId );
        }
        return dataRead().relationshipGetProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphGetProperty( long propertyKeyId )
    {
        statement.assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noGraphProperty( propertyKeyId );
        }
        return dataRead().graphGetProperty( statement, propertyKeyId );
    }

    @Override
    public Iterator<SafeProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().nodeGetAllProperties( statement, nodeId );
    }

    @Override
    public Iterator<SafeProperty> relationshipGetAllProperties( long relationshipId )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().relationshipGetAllProperties( statement, relationshipId );
    }

    @Override
    public Iterator<SafeProperty> graphGetAllProperties()
    {
        statement.assertOpen();
        return dataRead().graphGetAllProperties( statement );
    }
    // </DataRead>

    // <SchemaRead>
    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabelAndPropertyKey( statement, labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
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
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( statement, labelId );
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
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
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
    public long labelGetForName( String labelName )
    {
        statement.assertOpen();
        return tokenRead().labelGetForName( statement, labelName );
    }

    @Override
    public String labelGetName( long labelId ) throws LabelNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().labelGetName( statement, labelId );
    }

    @Override
    public long propertyKeyGetForName( String propertyKeyName )
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetForName( statement, propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( long propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetName( statement, propertyKeyId );
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().labelsGetAllTokens( statement );
    }

    @Override
    public long relationshipTypeGetForName( String relationshipTypeName )
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetForName( statement, relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( long relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetName( statement, relationshipTypeId );
    }
    // </TokenRead>

    // <TokenWrite>
    @Override
    public long labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        statement.assertOpen();
        return tokenWrite().labelGetOrCreateForName( statement, labelName );
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        return tokenWrite().propertyKeyGetOrCreateForName( statement, propertyKeyName );
    }

    @Override
    public long relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
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
    public boolean nodeAddLabel( long nodeId, long labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();
        return dataWrite().nodeAddLabel( statement, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveLabel( statement, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( long nodeId, SafeProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();
        return dataWrite().nodeSetProperty( statement, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, SafeProperty property )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().relationshipSetProperty( statement, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( SafeProperty property )
    {
        statement.assertOpen();
        return dataWrite().graphSetProperty( statement, property );
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().relationshipRemoveProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( long propertyKeyId )
    {
        statement.assertOpen();
        return dataWrite().graphRemoveProperty( statement, propertyKeyId );
    }
    // </DataWrite>

    // <SchemaWrite>
    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKeyId )
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
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
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
    // </SchemaWrite>
}
