package org.neo4j.kernel.impl.api;

import javax.transaction.Transaction;

import org.junit.Test;

import org.neo4j.helpers.Function2;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.WritableStatementState;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OldTxSafeStatementExecutorTest
{
    @Test
    public void shouldCloseResourcesAndCommitWhenSuccessful() throws Exception
    {
        // GIVEN
        AbstractTransactionManager transactionManager = mock( AbstractTransactionManager.class );
        when( transactionManager.suspend() ).thenReturn( null );
        final StatementState statementState = new WritableStatementState();
        when( transactionManager.newStatement() ).thenReturn( statementState );
        final StatementOperationParts statementLogic = mock( StatementOperationParts.class );

        OldTxSafeStatementExecutor executor = createNewExecutor( transactionManager, statementLogic );

        // WHEN
        executor.executeSingleStatement( new Function2<StatementState, StatementOperationParts, Object>()
        {
            @Override
            public Object apply( StatementState receivedState, StatementOperationParts receivedLogic )
            {
                assertEquals( statementState, receivedState );
                assertEquals( statementLogic, receivedLogic );
                return null;
            }
        } );

        // THEN
        verify( statementLogic ).close( statementState );
        verify( transactionManager ).commit();
    }

    @Test
    public void shouldCloseResourcesAndRollbackWhenUnsuccessful() throws Exception
    {
        // GIVEN
        AbstractTransactionManager transactionManager = mock( AbstractTransactionManager.class );
        when( transactionManager.suspend() ).thenReturn( null );
        final StatementState statementState = new WritableStatementState();
        when( transactionManager.newStatement() ).thenReturn( statementState );
        final StatementOperationParts statementLogic = mock( StatementOperationParts.class );

        OldTxSafeStatementExecutor executor = createNewExecutor( transactionManager, statementLogic );

        // WHEN
        try
        {
            executor.executeSingleStatement( new Function2<StatementState, StatementOperationParts, Object>()
            {
                @Override
                public Object apply( StatementState receivedState, StatementOperationParts receivedLogic )
                {
                    assertEquals( statementState, receivedState );
                    assertEquals( statementLogic, receivedLogic );
                    throw new RuntimeException( "I break your transaction!" );
                }
            } );
            fail( "Expected runtime execption to be thrown" );
        }
        catch ( RuntimeException e )
        {
            // We expect to get this
        }

        // THEN
        verify( statementLogic ).close( statementState );
        verify( transactionManager ).rollback();
    }

    @Test
    public void shouldSuspendAndResumeOngoingTransactionAroundExecutionWhenSuccessful() throws Exception
    {
        // GIVEN
        AbstractTransactionManager transactionManager = mock( AbstractTransactionManager.class );
        Transaction runningTransaction = mock( Transaction.class );
        when( transactionManager.suspend() ).thenReturn( runningTransaction );
        final StatementState statementState = new WritableStatementState();
        when( transactionManager.newStatement() ).thenReturn( statementState );
        final StatementOperationParts statementLogic = mock( StatementOperationParts.class );

        OldTxSafeStatementExecutor executor = createNewExecutor( transactionManager, statementLogic );

        // WHEN
        executor.executeSingleStatement( new Function2<StatementState, StatementOperationParts, Object>()
        {
            @Override
            public Object apply( StatementState receivedState, StatementOperationParts receivedLogic )
            {
                return null;
            }
        } );

        // THEN
        verify( transactionManager ).suspend();
        verify( statementLogic ).close( statementState );
        verify( transactionManager ).commit();
        verify( transactionManager ).resume( runningTransaction);
    }

    @Test
    public void shouldSuspendAndResumeOngoingTransactionAroundExecutionWhenUnsuccessful() throws Exception
    {
        // GIVEN
        AbstractTransactionManager transactionManager = mock( AbstractTransactionManager.class );
        Transaction runningTransaction = mock( Transaction.class );
        when( transactionManager.suspend() ).thenReturn( runningTransaction );
        final StatementState statementState = new WritableStatementState();
        when( transactionManager.newStatement() ).thenReturn( statementState );
        final StatementOperationParts statementLogic = mock( StatementOperationParts.class );

        OldTxSafeStatementExecutor executor = createNewExecutor( transactionManager, statementLogic );

        // WHEN
        try
        {
            executor.executeSingleStatement( new Function2<StatementState, StatementOperationParts, Object>()
            {
                @Override
                public Object apply( StatementState receivedState, StatementOperationParts receivedLogic )
                {
                    throw new RuntimeException( "I break your transaction!" );
                }
            } );
            fail( "Expected runtime execption to be thrown" );
        }
        catch ( RuntimeException e )
        {
            // We expect to get this
        }

        // THEN
        verify( transactionManager ).suspend();
        verify( statementLogic ).close( statementState );
        verify( transactionManager ).rollback();
        verify( transactionManager ).resume( runningTransaction);
    }

    private OldTxSafeStatementExecutor createNewExecutor( AbstractTransactionManager transactionManager,
                                                          final StatementOperationParts logic )
    {
        return new OldTxSafeStatementExecutor( transactionManager )
        {
            @Override
            public String toString()
            {
                return OldTxSafeStatementExecutor.class.getSimpleName();
            }

            @Override
            protected StatementOperationParts statementOperations()
            {
                return logic;
            }
        };
    }
}
