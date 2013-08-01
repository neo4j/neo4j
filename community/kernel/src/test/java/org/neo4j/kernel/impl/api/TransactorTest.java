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

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.junit.Test;
import org.mockito.InOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.BeginTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedParts;

@SuppressWarnings("deprecation")
public class TransactorTest
{
    @Test
    public void shouldCommitSuccessfulStatement() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        Transaction existingTransaction = mock( Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        KernelTransaction txContext = mock( KernelTransaction.class );
        when( txManager.getKernelTransaction() ).thenReturn( txContext );

        StatementOperationParts stmtContext = mockedParts( txContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        Object expectedResult = new Object();
        when( statement.perform( eq( stmtContext ), any( StatementState.class ) ) ).thenReturn( expectedResult );

        StatementState state = mock( StatementState.class );
        when( txContext.newStatementState() ).thenReturn( state );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( statement );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, txContext, state, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( txContext ).newStatementOperations();
        order.verify( statement ).perform( stmtContext, state );
        order.verify( state ).close();
        order.verify( txContext ).commit();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRollbackFailingStatement() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        Transaction existingTransaction = mock( Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        KernelTransaction txContext = mock( KernelTransaction.class );
        when( txManager.getKernelTransaction() ).thenReturn( txContext );

        StatementOperationParts stmtContext = mockedParts( txContext );
        when( txContext.newStatementOperations() ).thenReturn( stmtContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        SpecificKernelException exception = new SpecificKernelException();
        when( statement.perform( any( StatementOperationParts.class ), any( StatementState.class ) ) ).thenThrow( exception );

        StatementState state = mock( StatementState.class );
        when( txContext.newStatementState() ).thenReturn( state );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( statement );

            fail( "expected exception" );
        }
        // then
        catch ( SpecificKernelException e )
        {
            assertSame( exception, e );
        }
        InOrder order = inOrder( txManager, txContext, state, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( txContext ).newStatementOperations();
        order.verify( statement ).perform( stmtContext, state );
        order.verify( state ).close();
        order.verify( txContext ).rollback();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotResumeATransactionIfThereWasNoTransactionSuspended() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        KernelTransaction txContext = mock( KernelTransaction.class );
        when( txManager.getKernelTransaction() ).thenReturn( txContext );

        StatementOperationParts stmtContext = mockedParts( txContext );
        when( txContext.newStatementOperations() ).thenReturn( stmtContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        Object expectedResult = new Object();
        when( statement.perform( eq( stmtContext ), any( StatementState.class ) ) ).thenReturn( expectedResult );

        StatementState state = mock( StatementState.class );
        when( txContext.newStatementState() ).thenReturn( state );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( statement );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, txContext, state, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( txContext ).newStatementOperations();
        order.verify( statement ).perform( stmtContext, state );
        order.verify( state ).close();
        order.verify( txContext ).commit();
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPropagateNotSupportedExceptionFromBegin() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        when( txManager.suspend() ).thenReturn( mock( Transaction.class ) );
        NotSupportedException exception = new NotSupportedException();
        doThrow( exception ).when( txManager ).begin();

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( statement );

            fail( "expected exception" );
        }
        // then
        catch ( BeginTransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( statement );
        verify( txManager ).suspend();
        verify( txManager ).begin();
        verifyNoMoreInteractions( txManager );
    }

    @Test
    public void shouldPropagateSystemExceptionFromBegin() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        when( txManager.suspend() ).thenReturn( mock( Transaction.class ) );
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).begin();

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( statement );

            fail( "expected exception" );
        }
        // then
        catch ( BeginTransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( statement );
        verify( txManager ).suspend();
        verify( txManager ).begin();
        verifyNoMoreInteractions( txManager );
    }

    @Test
    public void shouldPropagateSystemExceptionFromSuspendTransaction() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).suspend();

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( statement );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( statement );
        verify( txManager ).suspend();
        verifyNoMoreInteractions( txManager );
    }

    @Test
    public void shouldPropagateSystemExceptionFromResumeTransaction() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).suspend();

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( statement );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( statement );
    }

    private static class SpecificKernelException extends KernelException
    {
        protected SpecificKernelException()
        {
            super( "very specific" );
        }
    }
}
