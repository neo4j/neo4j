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

import org.neo4j.helpers.ValueUtils;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.time.FakeClock;
import org.neo4j.values.virtual.MapValue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.security.auth.AuthenticationResult.AUTH_DISABLED;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class TransactionStateMachineTest
{
    private TransactionStateMachineSPI stateMachineSPI;
    private TransactionStateMachine.MutableTransactionState mutableState;
    private TransactionStateMachine stateMachine;

    @Before
    public void createMocks()
    {
        stateMachineSPI = mock( TransactionStateMachineSPI.class );
        mutableState = mock(TransactionStateMachine.MutableTransactionState.class);
        stateMachine = new TransactionStateMachine( stateMachineSPI, AUTH_DISABLED, new FakeClock() );
    }

    @Test
    public void shouldTransitionToExplicitTransactionOnBegin() throws Exception
    {
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "begin", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "BEGIN", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "   begin   ", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
        assertEquals( TransactionStateMachine.State.AUTO_COMMIT.run(
                mutableState, stateMachineSPI, "   BeGiN ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.EXPLICIT_TRANSACTION );
    }

    @Test
    public void shouldTransitionToAutoCommitOnCommit() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "commit", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "COMMIT", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   commit   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   CoMmIt ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldTransitionToAutoCommitOnRollback() throws Exception
    {
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "rollback", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "ROLLBACK", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   rollback   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
        assertEquals( TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                mutableState, stateMachineSPI, "   RoLlBaCk ;   ", EMPTY_MAP ),
                TransactionStateMachine.State.AUTO_COMMIT );
    }

    @Test
    public void shouldThrowOnBeginInExplicitTransaction() throws Exception
    {
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, stateMachineSPI, "begin", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("Nested transactions are not supported.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.EXPLICIT_TRANSACTION.run(
                    mutableState, stateMachineSPI, " BEGIN ", EMPTY_MAP );
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
                    mutableState, stateMachineSPI, "rollback", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to rollback.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, " ROLLBACK ", EMPTY_MAP );
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
                    mutableState, stateMachineSPI, "commit", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
        try
        {
            TransactionStateMachine.State.AUTO_COMMIT.run(
                    mutableState, stateMachineSPI, " COMMIT ", EMPTY_MAP );
        }
        catch ( QueryExecutionKernelException ex )
        {
            assertEquals("No current transaction to commit.", ex.getMessage() );
        }
    }

    @Test
    public void shouldNotWaitWhenNoBookmarkSupplied() throws Exception
    {
        stateMachine.run( "BEGIN", EMPTY_MAP );
        verify( stateMachineSPI, never() ).awaitUpToDate( anyLong() );
    }

    @Test
    public void shouldAwaitSingleBookmark() throws Exception
    {
        stateMachine.run( "BEGIN", map( "bookmark", "neo4j:bookmark:v1:tx15" ) );
        verify( stateMachineSPI ).awaitUpToDate( 15 );
    }

    @Test
    public void shouldAwaitMultipleBookmarks() throws Exception
    {
        MapValue params = map( "bookmarks", asList(
                "neo4j:bookmark:v1:tx15", "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx92", "neo4j:bookmark:v1:tx9" )
        );
        stateMachine.run( "BEGIN", params );
        verify( stateMachineSPI ).awaitUpToDate( 92 );
    }

    @Test
    public void shouldAwaitMultipleBookmarksWhenBothSingleAndMultipleSupplied() throws Exception
    {
        MapValue params = map(
                "bookmark", "neo4j:bookmark:v1:tx42",
                "bookmarks", asList( "neo4j:bookmark:v1:tx47", "neo4j:bookmark:v1:tx67", "neo4j:bookmark:v1:tx45" )
        );
        stateMachine.run( "BEGIN", params );
        verify( stateMachineSPI ).awaitUpToDate( 67 );
    }

    private MapValue map( Object... keyValues )
    {
        return ValueUtils.asMapValue( MapUtil.map( keyValues ) );
    }
}
