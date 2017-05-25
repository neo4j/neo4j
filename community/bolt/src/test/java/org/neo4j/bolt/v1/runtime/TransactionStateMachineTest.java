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
package org.neo4j.bolt.v1.runtime;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class TransactionStateMachineTest
{
    private TransactionStateMachine.SPI spi;
    private TransactionStateMachine.MutableTransactionState mutableState;

    @Before
    public void createMocks()
    {
        spi = mock(TransactionStateMachine.SPI.class, RETURNS_MOCKS);
        mutableState = mock(TransactionStateMachine.MutableTransactionState.class);
    }

    @Test
    public void shouldTransitionToExplicitTransactionOnBegin() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, spi, "begin", Collections.emptyMap() ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, spi, "BEGIN", Collections.emptyMap() ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, spi, "   begin   ", Collections.emptyMap() ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, spi, "   BeGiN ;   ", Collections.emptyMap() ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
    }

    @Test
    public void shouldTransitionToAutoCommitOnCommit() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "commit", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "COMMIT", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "   commit   ", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "   CoMmIt ;   ", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldTransitionToAutoCommitOnRollback() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "rollback", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "ROLLBACK", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "   rollback   ", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, spi, "   RoLlBaCk ;   ", Collections.emptyMap() ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldThrowOnBeginInExplicitTransaction() throws Exception
    {
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, spi, "begin", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("Nested transactions are not supported.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, spi, " BEGIN ", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("Nested transactions are not supported.", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowOnRollbackInAutoCommit() throws Exception
    {
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, spi, "rollback", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to rollback.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, spi, " ROLLBACK ", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to rollback.", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowOnCommitInAutoCommit() throws Exception
    {
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, spi, "commit", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, spi, " COMMIT ", Collections.emptyMap() );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
    }
}
