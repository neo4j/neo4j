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
package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.kernel.api.operations.LegacyKernelOperations;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class TransactionStatementSharingTest
{
    private KernelTransactionImplementation newTransaction()
    {
        return new KernelTransactionImplementation( mock( StatementOperationParts.class ),
                                                    mock( LegacyKernelOperations.class ) )
        {
            @Override
            protected void doCommit()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected void doRollback()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected Statement newStatement()
            {
                return mock( Statement.class );
            }
        };
    }

    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        ReadStatement stmt1 = tx.acquireReadStatement();

        // when
        ReadStatement stmt2 = tx.acquireReadStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndDataStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        ReadStatement stmt1 = tx.acquireReadStatement();

        // when
        DataStatement stmt2 = tx.acquireDataStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndSchemaStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        ReadStatement stmt1 = tx.acquireReadStatement();

        // when
        SchemaStatement stmt2 = tx.acquireSchemaStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentDataStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        DataStatement stmt1 = tx.acquireDataStatement();

        // when
        ReadStatement stmt2 = tx.acquireReadStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentDataStatementAndDataStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        DataStatement stmt1 = tx.acquireDataStatement();

        // when
        DataStatement stmt2 = tx.acquireDataStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentSchemaStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        SchemaStatement stmt1 = tx.acquireSchemaStatement();

        // when
        ReadStatement stmt2 = tx.acquireReadStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldShareStatementStateForConcurrentSchemaStatementAndSchemaStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        SchemaStatement stmt1 = tx.acquireSchemaStatement();

        // when
        SchemaStatement stmt2 = tx.acquireSchemaStatement();

        // then
        assertSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldNotShareStateForSequentialReadStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        ReadStatement stmt1 = tx.acquireReadStatement();
        stmt1.close();

        // when
        ReadStatement stmt2 = tx.acquireReadStatement();

        // then
        assertNotSame( stmt1.state, stmt2.state );
    }

    @Test
    public void shouldNotReleaseUnderlyingStatementIfClosingTheSameReadStatementTwice() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        ReadStatement stmt1 = tx.acquireReadStatement();
        ReadStatement stmt2 = tx.acquireReadStatement();
        stmt1.close();

        // when
        stmt1.close();
        ReadStatement stmt3 = tx.acquireReadStatement();

        // then
        assertSame( "stmt1 == stmt2", stmt1.state, stmt2.state );
        assertSame( "stmt2 == stmt3", stmt2.state, stmt3.state );
    }
}
