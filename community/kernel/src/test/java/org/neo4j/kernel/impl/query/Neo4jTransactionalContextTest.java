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
package org.neo4j.kernel.impl.query;

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;

import java.util.Optional;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutionStatisticsOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class Neo4jTransactionalContextTest
{
    private GraphDatabaseQueryService queryService;
    private Guard guard;
    private KernelStatement initialStatement;
    private ConfiguredPageCursorTracer tracer;
    private ThreadToStatementContextBridge txBridge;

    @Before
    public void setUp()
    {
        setUpMocks();
    }

    @Test
    public void checkKernelStatementOnCheck() throws Exception
    {
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );

        Neo4jTransactionalContext transactionalContext =
            new Neo4jTransactionalContext(
                null,
                null,
                guard,
                null,
                null,
                initialTransaction, initialStatement,
                null
            );

        transactionalContext.check();

        verify( guard ).check( initialStatement );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void neverStopsExecutingQueryDuringCommitAndRestartTx()
    {
        // Given
        KernelTransaction initialKTX = mock( KernelTransaction.class );
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        KernelTransaction.Type transactionType = KernelTransaction.Type.implicit;
        SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        when( initialTransaction.transactionType() ).thenReturn( transactionType );
        when( initialTransaction.securityContext() ).thenReturn( securityContext );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );
        QueryRegistryOperations initialQueryRegistry = mock( QueryRegistryOperations.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        PropertyContainerLocker locker = null;
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );

        KernelTransaction secondKTX = mock( KernelTransaction.class );
        InternalTransaction secondTransaction = mock( InternalTransaction.class );
        when( secondTransaction.terminationReason() ).thenReturn( Optional.empty() );
        Statement secondStatement = mock( Statement.class );
        QueryRegistryOperations secondQueryRegistry = mock( QueryRegistryOperations.class );

        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.queryParameters() ).thenReturn( EMPTY_MAP );
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( queryService.beginTransaction( transactionType, securityContext ) ).thenReturn( secondTransaction );
        when( txBridge.getKernelTransactionBoundToThisThread( true ) ).thenReturn( initialKTX, secondKTX );
        when( txBridge.get() ).thenReturn( secondStatement );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context = new Neo4jTransactionalContext( queryService,
                null,
                guard,
                txBridge,
                locker,
                initialTransaction,
                initialStatement,
                executingQuery
        );

        // When
        context.commitAndRestartTx();

        // Then
        Object[] mocks =
                { txBridge, initialTransaction, initialQueryRegistry, initialKTX, secondQueryRegistry, secondKTX };
        InOrder order = Mockito.inOrder( mocks );

        // (0) Constructor
        order.verify( initialTransaction ).transactionType();
        order.verify( initialTransaction ).securityContext();
        order.verify( initialTransaction ).terminationReason(); // not terminated check

        // (1) Unbind old
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (2) Register and unbind new
        order.verify( txBridge ).get();
        order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (3) Rebind, unregister, and close old
        order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
        order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
        order.verify( initialTransaction ).success();
        order.verify( initialTransaction ).close();
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (4) Rebind new
        order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
        verifyNoMoreInteractions( mocks );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void rollsBackNewlyCreatedTransactionIfTerminationDetectedOnCloseDuringPeriodicCommit()
    {
        // Given
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        KernelTransaction.Type transactionType = KernelTransaction.Type.implicit;
        SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        when( initialTransaction.transactionType() ).thenReturn( transactionType );
        when( initialTransaction.securityContext() ).thenReturn( securityContext );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );

        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        KernelTransaction initialKTX = mock( KernelTransaction.class );
        Statement initialStatement = mock( Statement.class );
        QueryRegistryOperations initialQueryRegistry = mock( QueryRegistryOperations.class );
        ExecutionStatisticsOperations initialExecutionStatisticOperations = mock( ExecutionStatisticsOperations.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        PropertyContainerLocker locker = new PropertyContainerLocker();
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );

        KernelTransaction secondKTX = mock( KernelTransaction.class );
        InternalTransaction secondTransaction = mock( InternalTransaction.class );
        when( secondTransaction.terminationReason() ).thenReturn( Optional.empty() );
        Statement secondStatement = mock( Statement.class );
        QueryRegistryOperations secondQueryRegistry = mock( QueryRegistryOperations.class );

        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.queryParameters() ).thenReturn( EMPTY_MAP );
        Mockito.doThrow( RuntimeException.class ).when( initialTransaction ).close();
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( initialStatement.executionStatisticsOperations() ).thenReturn( initialExecutionStatisticOperations );
        when( initialExecutionStatisticOperations.getPageCursorTracer() ).thenReturn( tracer );
        when( queryService.beginTransaction( transactionType, securityContext ) ).thenReturn( secondTransaction );
        when( txBridge.getKernelTransactionBoundToThisThread( true ) ).thenReturn( initialKTX, secondKTX );
        when( txBridge.get() ).thenReturn( secondStatement );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context = new Neo4jTransactionalContext(
            queryService,
            null,
            guard,
            txBridge,
            locker,
            initialTransaction,
            initialStatement,
            executingQuery
        );

        // When
        try
        {
            context.commitAndRestartTx();
            throw new AssertionError( "Expected RuntimeException to be thrown" );
        }
        catch ( RuntimeException e )
        {
            // Then
            Object[] mocks =
                { txBridge, initialTransaction, initialQueryRegistry, initialKTX,
                  secondQueryRegistry, secondKTX, secondTransaction };
            InOrder order = Mockito.inOrder( mocks );

            // (0) Constructor
            order.verify( initialTransaction ).transactionType();
            order.verify( initialTransaction ).securityContext();
            order.verify( initialTransaction ).terminationReason(); // not terminated check

            // (1) Unbind old
            order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            // (2) Register and unbind new
            order.verify( txBridge ).get();
            order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
            order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            // (3) Rebind, unregister, and close old
            order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
            order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
            order.verify( initialTransaction ).success();
            order.verify( initialTransaction ).close();
            order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
            order.verify( secondTransaction ).failure();
            order.verify( secondTransaction ).close();
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            verifyNoMoreInteractions( mocks );
        }
    }

    @Test
    public void accumulateExecutionStatisticOverCommitAndRestart()
    {
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );
        Neo4jTransactionalContext transactionalContext = new Neo4jTransactionalContext( queryService, null,
                guard, txBridge, null, initialTransaction,
                initialStatement, null );

        tracer.setFaults( 2 );
        tracer.setHits( 5 );

        transactionalContext.commitAndRestartTx();

        tracer.setFaults( 2 );
        tracer.setHits( 5 );

        transactionalContext.commitAndRestartTx();

        tracer.setFaults( 2 );
        tracer.setHits( 5 );

        StatisticProvider statisticProvider = transactionalContext.kernelStatisticProvider();

        assertEquals( "Expect to see accumulated number of page cache misses.",6, statisticProvider.getPageCacheMisses() );
        assertEquals( "Expected to see accumulated number of page cache hits.", 15, statisticProvider.getPageCacheHits() );
    }

    @Test
    public void shouldBeOpenAfterCreation()
    {
        InternalTransaction tx = mock( InternalTransaction.class );

        Neo4jTransactionalContext context = newContext( tx );

        assertTrue( context.isOpen() );
    }

    @Test
    public void shouldBeTopLevelWithImplicitTx()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        assertTrue( context.isTopLevelTx() );
    }

    @Test
    public void shouldNotBeTopLevelWithExplicitTx()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.explicit );

        Neo4jTransactionalContext context = newContext( tx );

        assertFalse( context.isTopLevelTx() );
    }

    @Test
    public void shouldNotCloseTransactionDuringTermination()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        verify( tx ).terminate();
        verify( tx, never() ).close();
    }

    @Test
    public void shouldBePossibleToCloseAfterTermination()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        verify( tx ).terminate();
        verify( tx, never() ).close();

        context.close( false );
        verify( tx ).failure();
        verify( tx ).close();
    }

    @Test
    public void shouldBePossibleToTerminateWithoutActiveTransaction()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        Neo4jTransactionalContext context = newContext( tx );

        context.close( true );
        verify( tx ).success();
        verify( tx ).close();

        context.terminate();
        verify( tx, never() ).terminate();
    }

    @Test
    public void shouldThrowWhenRestartedAfterTermination()
    {
        MutableObject<Status> terminationReason = new MutableObject<>();
        InternalTransaction tx = mock( InternalTransaction.class );
        doAnswer( invocation ->
        {
            terminationReason.setValue( Status.Transaction.Terminated );
            return null;
        } ).when( tx ).terminate();
        when( tx.terminationReason() ).then( invocation -> Optional.ofNullable( terminationReason.getValue() ) );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        try
        {
            context.commitAndRestartTx();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenGettingTxAfterTermination()
    {
        MutableObject<Status> terminationReason = new MutableObject<>();
        InternalTransaction tx = mock( InternalTransaction.class );
        doAnswer( invocation ->
        {
            terminationReason.setValue( Status.Transaction.Terminated );
            return null;
        } ).when( tx ).terminate();
        when( tx.terminationReason() ).then( invocation -> Optional.ofNullable( terminationReason.getValue() ) );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        try
        {
            context.getOrBeginNewIfClosed();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
        }
    }

    @Test
    public void shouldNotBePossibleToCloseMultipleTimes()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        Neo4jTransactionalContext context = newContext( tx );

        context.close( false );
        context.close( true );
        context.close( false );

        verify( tx ).failure();
        verify( tx, never() ).success();
        verify( tx ).close();
    }

    private void setUpMocks()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        DependencyResolver resolver = mock( DependencyResolver.class );
        txBridge = mock( ThreadToStatementContextBridge.class );
        guard = mock( Guard.class );
        initialStatement = mock( KernelStatement.class );
        tracer = new ConfiguredPageCursorTracer();
        QueryRegistryOperations queryRegistryOperations = mock( QueryRegistryOperations.class );
        ExecutionStatisticsOperations executionStatisticsOperations = mock( ExecutionStatisticsOperations.class );
        InternalTransaction internalTransaction = mock( InternalTransaction.class );
        when( internalTransaction.terminationReason() ).thenReturn( Optional.empty() );

        when( initialStatement.queryRegistration() ).thenReturn( queryRegistryOperations );
        when( initialStatement.executionStatisticsOperations() ).thenReturn( executionStatisticsOperations );
        when( executionStatisticsOperations.getPageCursorTracer() ).thenReturn( tracer );
        when( queryService.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( txBridge );
        when( resolver.resolveDependency( Guard.class ) ).thenReturn( guard );
        when( queryService.beginTransaction( any(), any()) ).thenReturn( internalTransaction );

        when( txBridge.get() ).thenReturn( initialStatement );
    }

    private Neo4jTransactionalContext newContext( InternalTransaction initialTx )
    {
        return new Neo4jTransactionalContext( queryService, null, guard,
                txBridge, new PropertyContainerLocker(), initialTx, initialStatement, null );
    }

    private class ConfiguredPageCursorTracer extends DefaultPageCursorTracer
    {
        private long hits;
        private long faults;

        @Override
        public long hits()
        {
            return super.hits() + hits;
        }

        @Override
        public long faults()
        {
            return super.faults() + faults;
        }

        void setHits( long hits )
        {
            this.hits = hits;
        }

        void setFaults( long faults )
        {
            this.faults = faults;
        }
    }
}
