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
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

public class BaseStatement implements TokenRead, TokenWrite, SchemaRead, SchemaState, AutoCloseable
{
    private final KernelTransactionImplementation transaction;
    private boolean closed;
    final Statement state;

    BaseStatement( KernelTransactionImplementation transaction, Statement state )
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
    public String propertyKeyGetName( long propertyKeyId ) throws PropertyKeyIdNotFoundException
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
    // </TokenRead>

    // <TokenWrite>
    @Override
    public long labelGetOrCreateForName( String labelName ) throws SchemaKernelException
    {
        assertOpen();
        return tokenWrite().labelGetOrCreateForName( state, labelName );
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKeyName ) throws SchemaKernelException
    {
        assertOpen();
        return tokenWrite().propertyKeyGetOrCreateForName( state, propertyKeyName );
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
