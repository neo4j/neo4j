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

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.StatementContextParts;
import org.neo4j.kernel.api.TransactionContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.StatementContextTestHelper.mockedParts;

public class ReferenceCountingTransactionContextTest
{
    private TransactionContext inner;
    private StatementContextParts actualContext;
    private StatementContextParts otherActualContext;
    private ReferenceCountingTransactionContext singleContext;

    @Before
    public void given() throws Exception
    {
        inner = mock( TransactionContext.class );
        actualContext = mockedParts();
        otherActualContext = mockedParts();
        when( inner.newStatementContext() )
                .thenReturn( actualContext )
                .thenReturn( otherActualContext );
        singleContext = new ReferenceCountingTransactionContext( inner );
    }

    @Test
    public void shouldOnlyCreateOneUnderlingStatementContext() throws Exception
    {
        // WHEN
        singleContext.newStatementContext();
        singleContext.newStatementContext();

        // THEN
        verify( inner, times( 1 ) ).newStatementContext();
    }

    @Test
    public void shouldNotCloseUnderlyingContextIfAnyoneIsStillUsingIt() throws Exception
    {
        // GIVEN
        StatementContextParts first = singleContext.newStatementContext();
        singleContext.newStatementContext();

        // WHEN
        first.close();

        // THEN
        verify( actualContext.lifecycleOperations(), never() ).close();
    }

    @Test
    public void closingAllStatementContextClosesUnderlyingContext() throws Exception
    {
        // GIVEN
        StatementContextParts first = singleContext.newStatementContext();
        StatementContextParts other = singleContext.newStatementContext();

        // WHEN
        first.close();
        other.close();

        // THEN
        verify( actualContext.lifecycleOperations(), times( 1 ) ).close();
    }

    @Test
    public void shouldOpenAndCloseTwoUnderlyingContextsWhenOpeningAndClosingTwoContextsInSequence() throws Exception
    {
        // WHEN
        singleContext.newStatementContext().close();
        singleContext.newStatementContext().close();

        // THEN
        verify( inner, times( 2 ) ).newStatementContext();
        verify( actualContext.lifecycleOperations() ).close();
        verify( otherActualContext.lifecycleOperations() ).close();
    }

    @Test
    public void shouldNotBeAbleToInteractWithAClosedStatementContext() throws Exception
    {
        // GIVEN
        StatementContextParts first = singleContext.newStatementContext();
        singleContext.newStatementContext();
        first.close();

        // WHEN
        try
        {
            first.asStatementContext().labelGetName( 0 );

            fail( "expected exception" );
        }
        // THEN
        catch ( IllegalStateException e )
        {
            assertEquals( "This StatementContext has been closed. No more interaction allowed", e.getMessage() );
        }
    }

    @Test
    public void shouldNotBeAbleToCloseTheSameStatementContextTwice() throws Exception
    {
        // GIVEN
        StatementContextParts first = singleContext.newStatementContext();
        singleContext.newStatementContext();
        first.close();

        // WHEN
        try
        {
            first.close();

            fail( "expected exception" );
        }
        // THEN
        catch ( IllegalStateException e )
        {
            assertEquals( "This StatementContext has been closed. No more interaction allowed", e.getMessage() );
        }
        verify( actualContext.lifecycleOperations(), never() ).close();
    }

    @Test
    public void shouldCloseAllStatementContextsOnCommit() throws Exception
    {
        // given
        StatementContextParts context = singleContext.newStatementContext();

        // when
        singleContext.commit();

        // then
        try
        {
            context.close();

            fail( "expected exception" );
        }
        catch ( IllegalStateException e )
        {
            assertEquals( "This StatementContext has been closed. No more interaction allowed", e.getMessage() );
        }
        verify( actualContext.lifecycleOperations(), times( 1 ) ).close();
    }

    @Test
    public void shouldCloseAllStatementContextsOnRollback() throws Exception
    {
        // given
        StatementContextParts context = singleContext.newStatementContext();

        // when
        singleContext.rollback();

        // then
        try
        {
            context.close();

            fail( "expected exception" );
        }
        catch ( IllegalStateException e )
        {
            assertEquals( "This StatementContext has been closed. No more interaction allowed", e.getMessage() );
        }
        verify( actualContext.lifecycleOperations(), times( 1 ) ).close();
    }

    @Test
    public void shouldBeAbleToOpenNewStatementContextsAfterCommit() throws Exception
    {
        // given
        StatementContextParts before = singleContext.newStatementContext();

        // when
        singleContext.commit();

        // then
        StatementContextParts after = singleContext.newStatementContext();

        try
        {
            before.close();

            fail( "expected exception" );
        }
        catch ( IllegalStateException e )
        {
            assertEquals( "This StatementContext has been closed. No more interaction allowed", e.getMessage() );
        }
        verify( actualContext.lifecycleOperations(), times( 1 ) ).close();
        verifyZeroInteractions( otherActualContext.lifecycleOperations() );
        after.close();
        verify( otherActualContext.lifecycleOperations(), times( 1 ) ).close();
    }
}
