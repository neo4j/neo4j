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

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.api.KernelTransactionImplementation;
import org.neo4j.kernel.api.MicroTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.Transactor;
import org.neo4j.kernel.api.exceptions.BeginTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

        javax.transaction.Transaction existingTransaction = mock( javax.transaction.Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        StatementOperationParts operations = mock( StatementOperationParts.class );
        Statement statement = mock( Statement.class );
        StubKernelTransaction kernelTransaction = spy( new StubKernelTransaction( operations, statement ) );
        when( txManager.getKernelTransaction() ).thenReturn( kernelTransaction );

        @SuppressWarnings("unchecked")
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );
        Object expectedResult = new Object();
        when( work.perform( eq( operations ), any( Statement.class ) ) ).thenReturn( expectedResult );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( work );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, kernelTransaction, statement, work );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( kernelTransaction ).newStatement();
        order.verify( work ).perform( operations, statement );
        order.verify( statement ).close();
        order.verify( kernelTransaction ).commit();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRollbackFailingStatement() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        javax.transaction.Transaction existingTransaction = mock( javax.transaction.Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        StatementOperationParts operations = mock( StatementOperationParts.class );
        Statement statement = mock( Statement.class );
        StubKernelTransaction kernelTransaction = spy( new StubKernelTransaction( operations, statement ) );
        when( txManager.getKernelTransaction() ).thenReturn( kernelTransaction );

        @SuppressWarnings("unchecked")
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );
        SpecificKernelException exception = new SpecificKernelException();
        when( work.perform( any( StatementOperationParts.class ), any( Statement.class ) ) ).thenThrow( exception );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( SpecificKernelException e )
        {
            assertSame( exception, e );
        }
        InOrder order = inOrder( txManager, kernelTransaction, operations, statement, work );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( kernelTransaction ).newStatement();
        order.verify( work ).perform( operations, statement );
        order.verify( statement ).close();
        order.verify( kernelTransaction ).rollback();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotResumeATransactionIfThereWasNoTransactionSuspended() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );

        when( txManager.suspend() ).thenReturn( null );

        StatementOperationParts operations = mock( StatementOperationParts.class );
        Statement statement = mock( Statement.class );
        StubKernelTransaction kernelTransaction = spy( new StubKernelTransaction( operations, statement ) );
        when( txManager.getKernelTransaction() ).thenReturn( kernelTransaction );

        @SuppressWarnings("unchecked")
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );
        Object expectedResult = new Object();
        when( work.perform( eq( operations ), any( Statement.class ) ) ).thenReturn( expectedResult );

        Transactor transactor = new Transactor( txManager );

        // when
        Object result = transactor.execute( work );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, operations, kernelTransaction, statement, work );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( txManager ).getKernelTransaction();
        order.verify( kernelTransaction ).execute( any( MicroTransaction.class ) );
        order.verify( kernelTransaction ).newStatement();
        order.verify( work ).perform( operations, statement );
        order.verify( statement ).close();
        order.verify( kernelTransaction ).commit();
        verifyNoMoreInteractions( txManager, operations, statement, work );
    }

    @Test
    public void shouldPropagateNotSupportedExceptionFromBegin() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        when( txManager.suspend() ).thenReturn( mock( javax.transaction.Transaction.class ) );
        NotSupportedException exception = new NotSupportedException();
        doThrow( exception ).when( txManager ).begin();

        @SuppressWarnings("unchecked")
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( BeginTransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( work );
        verify( txManager ).suspend();
        verify( txManager ).begin();
        verifyNoMoreInteractions( txManager );
    }

    @Test
    public void shouldPropagateSystemExceptionFromBegin() throws Exception
    {
        // given
        AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
        when( txManager.suspend() ).thenReturn( mock( javax.transaction.Transaction.class ) );
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).begin();

        @SuppressWarnings("unchecked")
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( BeginTransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( work );
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
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( work );
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
        Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );

        Transactor transactor = new Transactor( txManager );

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause() );
        }
        verifyZeroInteractions( work );
    }

    private static class SpecificKernelException extends KernelException
    {
        protected SpecificKernelException()
        {
            super( "very specific" );
        }
    }

    private class StubKernelTransaction extends KernelTransactionImplementation
    {
        private final Statement statement;

        protected StubKernelTransaction( StatementOperationParts operations, Statement statement )
        {
            super( operations, mock( LegacyKernelOperations.class ) );
            this.statement = statement;
        }

        @Override
        protected void doCommit() throws TransactionFailureException
        {
        }

        @Override
        protected void doRollback() throws TransactionFailureException
        {
        }

        @Override
        protected Statement newStatement()
        {
            return statement;
        }
    }
}
