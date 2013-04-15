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
package org.neo4j.server.rest.transactional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.deserilizationErrors;
import static org.neo4j.server.rest.transactional.StubStatementDeserializer.statements;

import org.junit.Test;
import org.mockito.InOrder;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.transactional.error.InvalidRequestError;

public class TransactionHandleTest
{
    @Test
    public void shouldExecuteStatements() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        TransactionHandle handle = new TransactionHandle( kernel, executionEngine,
                mock( TransactionRegistry.class ), StringLogger.DEV_NULL );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        verify( executionEngine ).execute( "query", map() );
    }

    @Test
    public void shouldSuspendTransactionAndReleaseForOtherRequestsAfterExecutingStatements() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, StringLogger.DEV_NULL );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        InOrder order = inOrder( transactionContext, registry );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337l, handle );
    }

    @Test
    public void shouldResumeTransactionWhenExecutingStatementsOnSecondRequest() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, StringLogger.DEV_NULL );

        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );
        reset( transactionContext, registry, executionEngine );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        InOrder order = inOrder( transactionContext, registry, executionEngine );
        order.verify( transactionContext ).resumeSinceTransactionsAreStillThreadBound();
        order.verify( executionEngine ).execute( "query", map() );
        order.verify( transactionContext ).suspendSinceTransactionsAreStillThreadBound();
        order.verify( registry ).release( 1337l, handle );
    }

    @Test
    public void shouldCommitTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, StringLogger.DEV_NULL );

        // when
        handle.commit( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        InOrder order = inOrder( transactionContext, registry );
        order.verify( transactionContext ).commit();
        order.verify( registry ).forget( 1337l );
    }

    @Test
    public void shouldRollbackTransactionAndTellRegistryToForgetItsHandle() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                registry, StringLogger.DEV_NULL );

        // when
        handle.rollback( new ValidatingResultHandler() );

        // then
        InOrder order = inOrder( transactionContext, registry );
        order.verify( transactionContext ).rollback();
        order.verify( registry ).forget( 1337l );
    }

    @Test
    public void shouldCreateTransactionContextOnlyWhenFirstNeeded() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();

        // when
        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ),
                mock( TransactionRegistry.class ), StringLogger.DEV_NULL );

        // then
        verifyZeroInteractions( kernel );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        verify( kernel ).newTransactionContext();
    }

    @Test
    public void shouldRollbackTransactionIfDeserializationErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ), registry, StringLogger.DEV_NULL );

        // when
        handle.execute( deserilizationErrors( new InvalidRequestError( "invalid request" ) ) , new ValidatingResultHandler() );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337l );
    }

    @Test
    public void shouldRollbackTransactionIfExecutionErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        ExecutionEngine executionEngine = mock( ExecutionEngine.class );
        when( executionEngine.execute( "query", map() ) ).thenThrow( new NullPointerException() );

        TransactionHandle handle = new TransactionHandle( kernel, executionEngine, registry, StringLogger.DEV_NULL );

        // when
        handle.execute( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        verify( transactionContext ).rollback();
        verify( registry ).forget( 1337l );
    }

    @Test
    public void shouldLogMessageIfCommitErrorOccurs() throws Exception
    {
        // given
        KernelAPI kernel = mockKernel();
        TransitionalTxManagementTransactionContext transactionContext =
                (TransitionalTxManagementTransactionContext) kernel.newTransactionContext();
        doThrow( new NullPointerException() ).when( transactionContext ).commit();

        StringLogger log = mock( StringLogger.class );

        TransactionRegistry registry = mock( TransactionRegistry.class );
        when( registry.begin() ).thenReturn( 1337l );

        TransactionHandle handle = new TransactionHandle( kernel, mock( ExecutionEngine.class ), registry, log );

        // when
        handle.commit( statements( new Statement( "query", map() ) ), new ValidatingResultHandler() );

        // then
        verify( log ).error( eq( "Failed to commit transaction." ), any( NullPointerException.class ) );
        verify( registry ).forget( 1337l );
    }

    private KernelAPI mockKernel()
    {
        TransitionalTxManagementTransactionContext context = mock( TransitionalTxManagementTransactionContext.class );
        KernelAPI kernel = mock( KernelAPI.class );
        when( kernel.newTransactionContext() ).thenReturn( context );
        return kernel;
    }
}
