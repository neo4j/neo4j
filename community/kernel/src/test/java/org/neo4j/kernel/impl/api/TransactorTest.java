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

import org.neo4j.kernel.api.BeginTransactionFailureException;
import org.neo4j.kernel.api.KernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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

        TransactionContext txContext = mock( TransactionContext.class );
        when( txManager.getTransactionContext() ).thenReturn( txContext );

        StatementContext stmtContext = mock( StatementContext.class );
        when( txContext.newStatementContext() ).thenReturn( stmtContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        Object expectedResult = new Object();
        when( statement.perform( stmtContext ) ).thenReturn( expectedResult );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( statement );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, txContext, stmtContext, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getTransactionContext();
        order.verify( txContext ).newStatementContext();
        order.verify( statement ).perform( stmtContext );
        order.verify( stmtContext ).close();
        order.verify( txContext ).commit();
        order.verify( txManager ).resume( existingTransaction );
        verifyNoMoreInteractions( txManager, txContext, stmtContext, statement );
    }

    @Test
    public void shouldRollbackFailingStatement() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        Transaction existingTransaction = mock( Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        TransactionContext txContext = mock( TransactionContext.class );
        when( txManager.getTransactionContext() ).thenReturn( txContext );

        StatementContext stmtContext = mock( StatementContext.class );
        when( txContext.newStatementContext() ).thenReturn( stmtContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        SpecificKernelException exception = new SpecificKernelException();
        when( statement.perform( any( StatementContext.class ) ) ).thenThrow( exception );

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
        InOrder order = inOrder( txManager, txContext, stmtContext, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getTransactionContext();
        order.verify( txContext ).newStatementContext();
        order.verify( statement ).perform( stmtContext );
        order.verify( stmtContext ).close();
        order.verify( txContext ).rollback();
        order.verify( txManager ).resume( existingTransaction );
        verifyNoMoreInteractions( txManager, txContext, stmtContext, statement );
    }

    @Test
    public void shouldNotResumeATransactionIfThereWasNoTransactionSuspended() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        TransactionContext txContext = mock( TransactionContext.class );
        when( txManager.getTransactionContext() ).thenReturn( txContext );

        StatementContext stmtContext = mock( StatementContext.class );
        when( txContext.newStatementContext() ).thenReturn( stmtContext );

        @SuppressWarnings("unchecked")
        Transactor.Statement<Object, KernelException> statement = mock( Transactor.Statement.class );
        Object expectedResult = new Object();
        when( statement.perform( stmtContext ) ).thenReturn( expectedResult );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( statement );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, txContext, stmtContext, statement );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getTransactionContext();
        order.verify( txContext ).newStatementContext();
        order.verify( statement ).perform( stmtContext );
        order.verify( stmtContext ).close();
        order.verify( txContext ).commit();
        verifyNoMoreInteractions( txManager, txContext, stmtContext, statement );
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
            super( null, "very specific" );
        }
    }
}
