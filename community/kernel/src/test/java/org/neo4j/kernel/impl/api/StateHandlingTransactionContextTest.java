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
package org.neo4j.kernel.impl.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

import java.util.Map;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class StateHandlingTransactionContextTest
{
    @Test
    public void should_not_flush_schema_state_changes_until_commit() throws ConstraintViolationKernelException
    {
        // GIVEN A STATE HOLDER
        KernelSchemaStateStore schemaState = mock( KernelSchemaStateStore.class );

        // GIVEN AN INNER STATEMENT CONTEXT
        IndexRule rule = new IndexRule( 0L, 0L, PROVIDER_DESCRIPTOR, 1L );
        StatementContext innerStatementContext = mock( StatementContext.class );

        // GIVEN A TRANSACTION CONTEXT
        TransactionContext inner = mock( TransactionContext.class );
        when( inner.newStatementContext() ).thenReturn( innerStatementContext );
        PersistenceCache persistenceCache = mock( PersistenceCache.class );
        TransactionState oldState = null;
        TxState txState = mock( TxState.class );
        when( txState.haveIndexesBeenDropped() ).thenReturn( true );
        SchemaCache schemaCache = null;

        StateHandlingTransactionContext transactionContext =
                new StateHandlingTransactionContext( inner, txState, persistenceCache, oldState,
                        schemaCache, schemaState, mock( NodeManager.class ) );

        // GIVEN A STATEMENT CONTEXT DERIVED FROM THE TRANSACTION CONTEXT
        StatementContext statementContext = transactionContext.newStatementContext();

        // WHEN UPDATING THE SCHEMA
        statementContext.dropIndexRule( rule );

        // THEN
        verifyZeroInteractions( schemaState );

        // WHEN
        transactionContext.commit();

        // THEN
        verify( schemaState ).flush();
    }

    private static class UpdateHolderMap implements Answer<String>
    {
        @Override
        public String answer( InvocationOnMock invocation ) throws Throwable
        {
            Object[] arguments = invocation.getArguments();
            String key = (String) arguments[0];
            Function<String, String> creator = (Function<String, String>) arguments[2];
            Map<Object, Object> targetMap = (Map<Object, Object>) arguments[3];

            String value = creator.apply( key );
            targetMap.put( key, value );
            return value;
        }
    }
}
