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
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

public class ReadStatement implements TokenRead, TokenWrite, DataRead, SchemaRead, SchemaState, AutoCloseable
{
    private final KernelTransactionImplementation transaction;
    private boolean closed;
    final Statement state;

    ReadStatement( KernelTransactionImplementation transaction, Statement state )
    {
        this.transaction = transaction;
        this.state = state;
    }

    void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "The statement has been closed." );
        }
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            closed = true;
            transaction.releaseStatement( state );
        }
    }

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

    final SchemaWriteOperations schemaWrite()
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
        assertOpen();
        if ( labelId == NO_SUCH_LABEL )
        {
            return emptyPrimitiveLongIterator();
        }
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
        return labelId != NO_SUCH_LABEL && dataRead().nodeHasLabel( state, nodeId, labelId );
    }

    @Override
    public PrimitiveLongIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        assertOpen();
        return dataRead().nodeGetLabels( state, nodeId );
    }

    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noNodeProperty( nodeId, propertyKeyId );
        }
        return dataRead().nodeGetProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noRelationshipProperty( relationshipId, propertyKeyId );
        }
        return dataRead().relationshipGetProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphGetProperty( long propertyKeyId )
    {
        assertOpen();
        if ( propertyKeyId == NO_SUCH_PROPERTY_KEY )
        {
            return Property.noGraphProperty( propertyKeyId );
        }
        return dataRead().graphGetProperty( state, propertyKeyId );
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

    // <SchemaRead>
    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey )
            throws SchemaRuleNotFoundException
    {
        assertOpen();
        return schemaRead().indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        assertOpen();
        return schemaRead().indexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        assertOpen();
        return schemaRead().indexesGetAll( state );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        assertOpen();
        return schemaRead().uniqueIndexesGetAll( state );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        assertOpen();
        return schemaRead().indexGetState( state, descriptor );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        assertOpen();
        return schemaRead().indexGetFailure( state, descriptor );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        assertOpen();
        return schemaRead().constraintsGetForLabel( state, labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        assertOpen();
        return schemaRead().constraintsGetAll( state );
    }
    // </SchemaRead>

    // <TokenRead>
    @Override
    public long labelGetForName( String labelName )
    {
        assertOpen();
        return tokenRead().labelGetForName( state, labelName );
    }

    @Override
    public String labelGetName( long labelId ) throws LabelNotFoundKernelException
    {
        assertOpen();
        return tokenRead().labelGetName( state, labelId );
    }

    @Override
    public long propertyKeyGetForName( String propertyKeyName )
    {
        assertOpen();
        return tokenRead().propertyKeyGetForName( state, propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( long propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        assertOpen();
        return tokenRead().propertyKeyGetName( state, propertyKeyId );
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        assertOpen();
        return tokenRead().labelsGetAllTokens( state );
    }

    @Override
    public long relationshipTypeGetForName( String relationshipTypeName )
    {
        assertOpen();
        return tokenRead().relationshipTypeGetForName( state, relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( long relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        assertOpen();
        return tokenRead().relationshipTypeGetName( state, relationshipTypeId );
    }
    // </TokenRead>

    // <TokenWrite>
    @Override
    public long labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        assertOpen();
        return tokenWrite().labelGetOrCreateForName( state, labelName );
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        assertOpen();
        return tokenWrite().propertyKeyGetOrCreateForName( state, propertyKeyName );
    }

    @Override
    public long relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        assertOpen();
        return tokenWrite().relationshipTypeGetOrCreateForName( state, relationshipTypeName );
    }
    // </TokenWrite>

    // <SchemaState>
    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        return schemaState().schemaStateGetOrCreate( state, key, creator );
    }
    // </SchemaState>
}
