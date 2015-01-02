/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
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

public class TransactorTest
{
    private final AbstractTransactionManager txManager = mock( AbstractTransactionManager.class );
    private final PersistenceManager pm = mock(PersistenceManager.class);
    private final Statement statement = mock( Statement.class );
    private final KernelTransaction kernelTransaction = mock( KernelTransaction.class );
    @SuppressWarnings("unchecked")
    private final Transactor.Work<Object, KernelException> work = mock( Transactor.Work.class );
    private final Transactor transactor = new Transactor( txManager, pm );

    @Test
    public void shouldCommitSuccessfulStatement() throws Exception
    {
        // given
        javax.transaction.Transaction existingTransaction = mock( javax.transaction.Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        when( pm.currentKernelTransactionForWriting() ).thenReturn( kernelTransaction );
        when( kernelTransaction.acquireStatement() ).thenReturn( statement );

        Object expectedResult = new Object();
        when( work.perform( statement ) ).thenReturn( expectedResult );

        // when
        Object result = transactor.execute( work );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, pm, kernelTransaction );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( pm ).currentKernelTransactionForWriting();
        order.verify( txManager ).commit();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRollbackFailingStatement() throws Exception
    {
        // given
        javax.transaction.Transaction existingTransaction = mock( javax.transaction.Transaction.class );
        when( txManager.suspend() ).thenReturn( existingTransaction );

        when( pm.currentKernelTransactionForWriting()  ).thenReturn( kernelTransaction );
        when( kernelTransaction.acquireStatement() ).thenReturn( statement );

        SpecificKernelException exception = new SpecificKernelException();
        when( work.perform( any( KernelStatement.class ) ) ).thenThrow( exception );

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
        InOrder order = inOrder( txManager, pm, kernelTransaction, work );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( pm ).currentKernelTransactionForWriting();
        order.verify( txManager ).rollback();
        order.verify( txManager ).resume( existingTransaction );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotResumeATransactionIfThereWasNoTransactionSuspended() throws Exception
    {
        // given
        when( txManager.suspend() ).thenReturn( null );

        when( pm.currentKernelTransactionForWriting()  ).thenReturn( kernelTransaction );
        when( kernelTransaction.acquireStatement() ).thenReturn( statement );

        Object expectedResult = new Object();
        when( work.perform( statement ) ).thenReturn( expectedResult );

        // when
        Object result = transactor.execute( work );

        // then
        assertEquals( expectedResult, result );
        InOrder order = inOrder( txManager, pm, kernelTransaction, work );
        order.verify( txManager ).suspend();
        order.verify( txManager ).begin();
        order.verify( pm ).currentKernelTransactionForWriting();
        order.verify( work ).perform( statement );
        order.verify( txManager ).commit();
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPropagateNotSupportedExceptionFromBegin() throws Exception
    {
        // given
        when( txManager.suspend() ).thenReturn( mock( javax.transaction.Transaction.class ) );
        NotSupportedException exception = new NotSupportedException();
        doThrow( exception ).when( txManager ).begin();

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause().getCause() );
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
        when( txManager.suspend() ).thenReturn( mock( javax.transaction.Transaction.class ) );
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).begin();

        // when
        try
        {
            transactor.execute( work );

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            assertSame( exception, e.getCause().getCause() );
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
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).suspend();

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
        SystemException exception = new SystemException();
        doThrow( exception ).when( txManager ).suspend();

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
            super( Status.General.UnknownFailure, "very specific" );
        }
    }
}
