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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

public class SingleStatementTransactionContextTest
{
    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowNewStatementContextWhenTheOldIsOpen() throws Exception
    {
        // GIVEN
        TransactionContext actualTransactionContext = mock( TransactionContext.class );
        StatementContext actualStatement = mock( StatementContext.class );
        when( actualTransactionContext.newStatementContext() ).thenReturn( actualStatement );
        TransactionContext singleContext = new SingleStatementTransactionContext( actualTransactionContext );

        // WHEN
        singleContext.newStatementContext();
        singleContext.newStatementContext();
    }
    
    @Test( expected = IllegalStateException.class )
    public void shouldNotBeAbleToCloseTxWhenStatementIsOpen() throws Exception
    {
        // GIVEN
        TransactionContext actual = mock( TransactionContext.class );
        TransactionContext transactionContext = new SingleStatementTransactionContext( actual );
        transactionContext.newStatementContext();

        // WHEN
        transactionContext.finish();
    }

    @Test
    public void shouldBeAbleStartNewStatementAfterPreviousIsClosed() throws Exception
    {
        // GIVEN
        TransactionContext actual = mock( TransactionContext.class );
        StatementContext actualStatement = mock( StatementContext.class );
        when( actual.newStatementContext() ).thenReturn( actualStatement );

        TransactionContext transactionContext = new SingleStatementTransactionContext( actual );
        StatementContext firstStatement = transactionContext.newStatementContext();
        firstStatement.close();

        // WHEN
        StatementContext secondStatement = transactionContext.newStatementContext();

        // No exception is closed.
    }
}
