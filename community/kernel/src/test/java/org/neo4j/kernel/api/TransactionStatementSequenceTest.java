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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TransactionStatementSequenceTest
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
    public void shouldAllowReadStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireReadStatement().close();

        // when / then
        tx.acquireReadStatement().close();
    }

    @Test
    public void shouldAllowDataStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireReadStatement().close();

        // when / then
        tx.acquireDataStatement().close();
    }

    @Test
    public void shouldAllowSchemaStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireReadStatement().close();

        // when / then
        tx.acquireSchemaStatement().close();
    }

    @Test
    public void shouldRejectSchemaStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireDataStatement().close();

        // when
        try
        {
            tx.acquireSchemaStatement().close();

            fail( "expected exception" );
        }
        // then
        catch ( InvalidTransactionTypeException e )
        {
            assertEquals( "Cannot perform schema updates in a transaction that has performed data updates.",
                          e.getMessage() );
        }
    }

    @Test
    public void shouldRejectDataStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireSchemaStatement().close();

        // when
        try
        {
            tx.acquireDataStatement().close();

            fail( "expected exception" );
        }
        // then
        catch ( InvalidTransactionTypeException e )
        {
            assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.",
                          e.getMessage() );
        }
    }

    @Test
    public void shouldAllowDataStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireDataStatement().close();

        // when / then
        tx.acquireDataStatement().close();
    }

    @Test
    public void shouldAllowSchemaStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireSchemaStatement().close();

        // when / then
        tx.acquireSchemaStatement().close();
    }

    @Test
    public void shouldAllowReadStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireDataStatement().close();

        // when / then
        tx.acquireReadStatement().close();
    }

    @Test
    public void shouldAllowReadStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransactionImplementation tx = newTransaction();
        tx.acquireSchemaStatement().close();

        // when / then
        tx.acquireReadStatement().close();
    }
}
