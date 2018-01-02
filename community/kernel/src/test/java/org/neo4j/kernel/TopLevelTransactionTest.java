/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Test;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopLevelTransactionTest
{
    @Test
    public void shouldThrowTransientExceptionOnTransientKernelException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransactionFailureException( Status.Transaction.ConstraintsChanged,
                "Proving that TopLevelTransaction does the right thing" ) ).when( kernelTransaction ).close();
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge();
        TopLevelTransaction transaction = new TopLevelTransaction( kernelTransaction, bridge );

        // WHEN
        transaction.success();
        try
        {
            transaction.close();
            fail( "Should have failed" );
        }
        catch ( TransientTransactionFailureException e )
        {   // THEN Good
        }
    }

    @Test
    public void shouldThrowTransactionExceptionOnTransientKernelException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new RuntimeException( "Just a random failure" ) ).when( kernelTransaction ).close();
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge();
        TopLevelTransaction transaction = new TopLevelTransaction( kernelTransaction, bridge );

        // WHEN
        transaction.success();
        try
        {
            transaction.close();
            fail( "Should have failed" );
        }
        catch ( org.neo4j.graphdb.TransactionFailureException e )
        {   // THEN Good
        }
    }

    @Test
    public void shouldLetThroughTransientFailureException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransientDatabaseFailureException( "Just a random failure" ) ).when( kernelTransaction ).close();
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge();
        TopLevelTransaction transaction = new TopLevelTransaction( kernelTransaction, bridge );

        // WHEN
        transaction.success();
        try
        {
            transaction.close();
            fail( "Should have failed" );
        }
        catch ( TransientFailureException e )
        {   // THEN Good
        }
    }

    @Test
    public void shouldShowTransactionTerminatedExceptionAsTransient() throws Exception
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        doReturn( true ).when( kernelTransaction ).isOpen();
        RuntimeException error = new TransactionTerminatedException( Status.Transaction.Terminated );
        doThrow( error ).when( kernelTransaction ).close();
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge();
        TopLevelTransaction transaction = new TopLevelTransaction( kernelTransaction, bridge );

        transaction.success();
        try
        {
            transaction.close();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TransientTransactionFailureException.class ) );
            assertSame( error, e.getCause() );
        }
    }
}
