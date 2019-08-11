/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;

import java.util.Optional;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.kernel.impl.util.DefaultValueMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class Neo4jTransactionalContextTest
{
    private GraphDatabaseQueryService queryService;
    private KernelStatement initialStatement;
    private ThreadToStatementContextBridge txBridge;
    private ConfiguredExecutionStatistics statistics;
    private GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
    private DefaultValueMapper valueMapper = new DefaultValueMapper( databaseFacade );
    private KernelTransactionFactory transactionFactory = mock( KernelTransactionFactory.class );
    private DatabaseId databaseId = TestDatabaseIdRepository.randomDatabaseId();

    @BeforeEach
    void setUp()
    {
        setUpMocks();
    }

    @Test
    void checkKernelStatementOnCheck()
    {
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        when( executingQuery.databaseId() ).thenReturn( databaseId );
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
        KernelTransaction kernelTransaction = mockTransaction( initialStatement );
        when( txBridge.getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) ) ).thenReturn( kernelTransaction );

        Neo4jTransactionalContext transactionalContext =
                new Neo4jTransactionalContext( null, txBridge, initialTransaction, initialStatement, executingQuery, valueMapper, transactionFactory );

        transactionalContext.check();

        verify( kernelTransaction ).assertOpen();
    }

    @Test
    void neverStopsExecutingQueryDuringCommitAndRestartTx() throws TransactionFailureException
    {
        // Given
        KernelTransaction initialKTX = mockTransaction( initialStatement );
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        KernelTransaction.Type transactionType = KernelTransaction.Type.implicit;
        SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        ClientConnectionInfo connectionInfo = ClientConnectionInfo.EMBEDDED_CONNECTION;
        when( initialTransaction.transactionType() ).thenReturn( transactionType );
        when( initialTransaction.securityContext() ).thenReturn( securityContext );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );
        when( initialTransaction.clientInfo() ).thenReturn( connectionInfo );
        QueryRegistry initialQueryRegistry = mock( QueryRegistry.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );

        Statement secondStatement = mock( Statement.class );
        KernelTransaction secondKTX = mockTransaction( secondStatement );
        QueryRegistry secondQueryRegistry = mock( QueryRegistry.class );

        when( transactionFactory.beginKernelTransaction( transactionType, securityContext, connectionInfo ) ).thenReturn( secondKTX );
        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.databaseId() ).thenReturn( databaseId );
        when( executingQuery.queryParameters() ).thenReturn( EMPTY_MAP );
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( txBridge.getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) ) ).thenReturn(
                initialKTX,
                initialKTX,
                secondKTX );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context =
                new Neo4jTransactionalContext( queryService, txBridge, initialTransaction, initialStatement, executingQuery, valueMapper, transactionFactory );

        // When
        context.commitAndRestartTx();

        // Then
        Object[] mocks =
                {txBridge, initialTransaction, initialKTX, initialQueryRegistry, secondQueryRegistry, secondKTX};
        InOrder order = Mockito.inOrder( mocks );

        // (0) Constructor
        order.verify( initialTransaction ).transactionType();
        order.verify( initialTransaction ).securityContext();
        order.verify( initialTransaction ).clientInfo();
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) );
        order.verify( initialTransaction ).terminationReason(); // not terminated check

        // (1) Collect stats
        order.verify( initialKTX ).executionStatistics();

        // (2) Unbind old
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (3) Register and unbind new
        order.verify( secondKTX ).acquireStatement( );
        order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (4) Rebind, unregister, and close old
        order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
        order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
        order.verify( initialKTX ).commit();
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (5) Rebind new
        order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void rollsBackNewlyCreatedTransactionIfTerminationDetectedOnCloseDuringPeriodicCommit() throws TransactionFailureException
    {
        // Given
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        KernelTransaction.Type transactionType = KernelTransaction.Type.implicit;
        SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        ClientConnectionInfo connectionInfo = ClientConnectionInfo.EMBEDDED_CONNECTION;
        when( initialTransaction.transactionType() ).thenReturn( transactionType );
        when( initialTransaction.clientInfo() ).thenReturn( connectionInfo );
        when( initialTransaction.securityContext() ).thenReturn( securityContext );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );

        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        Statement initialStatement = mock( Statement.class );
        KernelTransaction initialKTX = mockTransaction( initialStatement );
        QueryRegistry initialQueryRegistry = mock( QueryRegistry.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );

        Statement secondStatement = mock( Statement.class );
        KernelTransaction secondKTX = mockTransaction( secondStatement );
        QueryRegistry secondQueryRegistry = mock( QueryRegistry.class );

        when( transactionFactory.beginKernelTransaction( transactionType, securityContext, connectionInfo ) ).thenReturn( secondKTX );
        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.databaseId() ).thenReturn( databaseId );
        when( executingQuery.queryParameters() ).thenReturn( EMPTY_MAP );
        Mockito.doThrow( RuntimeException.class ).when( initialKTX ).commit();
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( txBridge.getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) ) ).thenReturn( initialKTX, initialKTX, secondKTX );
        when( txBridge.get( any( DatabaseId.class ) ) ).thenReturn( secondStatement );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context =
                new Neo4jTransactionalContext( queryService, txBridge, initialTransaction, initialStatement, executingQuery, valueMapper, transactionFactory );

        // When
        assertThrows(RuntimeException.class, context::commitAndRestartTx );

        Object[] mocks =
                {txBridge, initialTransaction, initialQueryRegistry, initialKTX,
                        secondQueryRegistry, secondKTX};
        InOrder order = Mockito.inOrder( mocks );

        // (0) Constructor
        order.verify( initialTransaction ).transactionType();
        order.verify( initialTransaction ).securityContext();
        order.verify( initialTransaction ).clientInfo();
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) );
        order.verify( initialTransaction ).terminationReason(); // not terminated check

        // (1) Collect statistics
        order.verify( initialKTX ).executionStatistics();

        // (2) Unbind old
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (3) Register and unbind new
        order.verify( secondKTX ).acquireStatement();
        order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (4) Rebind, unregister, and close old
        order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
        order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
        order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
        order.verify( initialTransaction ).rollback();
        order.verify( txBridge ).unbindTransactionFromCurrentThread();
    }

    @Test
    void accumulateExecutionStatisticOverCommitAndRestart()
    {
        InternalTransaction initialTransaction = mock( InternalTransaction.class, new ReturnsDeepStubs() );
        when( initialTransaction.terminationReason() ).thenReturn( Optional.empty() );
        var statementMock = mock( Statement.class, new ReturnsDeepStubs() );
        var transaction = mockTransaction( statementMock );
        when( transactionFactory.beginKernelTransaction( any(), any(), any() ) ).thenReturn( transaction );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        when( executingQuery.databaseId() ).thenReturn( databaseId );
        Neo4jTransactionalContext transactionalContext = new Neo4jTransactionalContext( queryService,
                txBridge, initialTransaction, initialStatement, executingQuery, valueMapper, transactionFactory );

        statistics.setFaults( 2 );
        statistics.setHits( 5 );

        transactionalContext.commitAndRestartTx();

        statistics.setFaults( 2 );
        statistics.setHits( 5 );

        transactionalContext.commitAndRestartTx();

        statistics.setFaults( 2 );
        statistics.setHits( 5 );

        StatisticProvider statisticProvider = transactionalContext.kernelStatisticProvider();

        assertEquals( 6, statisticProvider.getPageCacheMisses(), "Expect to see accumulated number of page cache misses." );
        assertEquals( 15, statisticProvider.getPageCacheHits(), "Expected to see accumulated number of page cache hits." );
    }

    @Test
    void shouldBeOpenAfterCreation()
    {
        InternalTransaction tx = mock( InternalTransaction.class );

        Neo4jTransactionalContext context = newContext( tx );

        assertTrue( context.isOpen() );
    }

    @Test
    void shouldBeTopLevelWithImplicitTx()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        assertTrue( context.isTopLevelTx() );
    }

    @Test
    void shouldNotBeTopLevelWithExplicitTx()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.explicit );

        Neo4jTransactionalContext context = newContext( tx );

        assertFalse( context.isTopLevelTx() );
    }

    @Test
    void shouldNotCloseTransactionDuringTermination()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        verify( tx ).terminate();
        verify( tx, never() ).close();
    }

    @Test
    void shouldBePossibleToCloseAfterTermination()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        when( tx.transactionType() ).thenReturn( KernelTransaction.Type.implicit );

        Neo4jTransactionalContext context = newContext( tx );

        context.terminate();

        verify( tx ).terminate();
        verify( tx, never() ).close();

        context.close();
    }

    @Test
    void shouldBePossibleToTerminateWithoutActiveTransaction()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        Neo4jTransactionalContext context = newContext( tx );

        context.close();

        context.terminate();
        verify( tx, never() ).terminate();
    }

    @Test
    void shouldThrowWhenRestartedAfterTermination()
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

        assertThrows( TransactionTerminatedException.class, context::commitAndRestartTx );
    }

    @Test
    void shouldThrowWhenGettingTxAfterTermination()
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

        assertThrows( TransactionTerminatedException.class, context::getOrBeginNewIfClosed );
    }

    @Test
    void shouldNotBePossibleToCloseMultipleTimes()
    {
        InternalTransaction tx = mock( InternalTransaction.class );
        Neo4jTransactionalContext context = newContext( tx );

        context.close();
        context.close();
        context.close();

    }

    private void setUpMocks()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        DependencyResolver resolver = mock( DependencyResolver.class );
        txBridge = mock( ThreadToStatementContextBridge.class );
        initialStatement = mock( KernelStatement.class );

        statistics = new ConfiguredExecutionStatistics();
        QueryRegistry queryRegistry = mock( QueryRegistry.class );
        InternalTransaction internalTransaction = mock( InternalTransaction.class );
        when( internalTransaction.terminationReason() ).thenReturn( Optional.empty() );

        when( initialStatement.queryRegistration() ).thenReturn( queryRegistry );
        when( queryService.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( txBridge );
        when( queryService.beginTransaction( any(), any(), any() ) ).thenReturn( internalTransaction );

        KernelTransaction mockTransaction = mockTransaction( initialStatement );
        when( txBridge.get( any( DatabaseId.class ) ) ).thenReturn( initialStatement );
        when( txBridge.getKernelTransactionBoundToThisThread( eq( true ), any( DatabaseId.class ) ) ).thenReturn( mockTransaction );
    }

    private Neo4jTransactionalContext newContext( InternalTransaction initialTx )
    {
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        when( executingQuery.databaseId() ).thenReturn( databaseId );
        return new Neo4jTransactionalContext( queryService, txBridge, initialTx, initialStatement, executingQuery, valueMapper, transactionFactory );
    }

    private KernelTransaction mockTransaction( Statement statement )
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class, new ReturnsDeepStubs() );
        when( kernelTransaction.executionStatistics() ).thenReturn( statistics );
        when( kernelTransaction.acquireStatement() ).thenReturn( statement );
        return kernelTransaction;
    }

    private static class ConfiguredExecutionStatistics implements ExecutionStatistics
    {
        private long hits;
        private long faults;

        @Override
        public long pageHits()
        {
            return hits;
        }

        @Override
        public long pageFaults()
        {
            return faults;
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
