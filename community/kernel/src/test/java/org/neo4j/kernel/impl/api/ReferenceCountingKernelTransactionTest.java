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
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.operations.StatementState;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ReferenceCountingKernelTransactionTest
{
    private KernelTransaction inner;
    private StatementState actualState;
    private StatementState otherActualState;
    private ReferenceCountingKernelTransaction refCountingContext;

    @Before
    public void given() throws Exception
    {
        inner = mock( KernelTransaction.class );
        actualState = mock( StatementState.class );
        otherActualState = mock( StatementState.class );
        when( inner.newStatementState() )
                .thenReturn( actualState )
                .thenReturn( otherActualState );
        when( inner.newStatementOperations() )
                .thenReturn( new StatementOperationParts( null, null, null, null, null, null, null ) );
        refCountingContext = new ReferenceCountingKernelTransaction( inner );
    }

    @Test
    public void shouldOnlyCreateOneUnderlingStatementContext() throws Exception
    {
        // WHEN
        refCountingContext.newStatementState();
        refCountingContext.newStatementState();

        // THEN
        verify( inner, times( 1 ) ).newStatementState();
    }

    @Test
    public void shouldNotCloseUnderlyingContextIfAnyoneIsStillUsingIt() throws Exception
    {
        // GIVEN
        StatementState first = refCountingContext.newStatementState();
        refCountingContext.newStatementState();

        // WHEN
        first.close();

        // THEN
        verify( actualState, never() ).close();
    }

    @Test
    public void closingAllStatementContextClosesUnderlyingContext() throws Exception
    {
        // GIVEN
        StatementState first = refCountingContext.newStatementState();
        StatementState other = refCountingContext.newStatementState();

        // WHEN
        first.close();
        other.close();

        // THEN
        verify( actualState, times( 1 ) ).close();
    }

    @Test
    public void shouldOpenAndCloseTwoUnderlyingContextsWhenOpeningAndClosingTwoContextsInSequence() throws Exception
    {
        // WHEN
        refCountingContext.newStatementState().close();
        refCountingContext.newStatementState().close();


        // THEN
        verify( inner, times( 2 ) ).newStatementState();
        verify( actualState ).close();
        verify( otherActualState ).close();
    }

    @Ignore( "Not valid I(MP)'d say, we don't check that anymore. Such checks are costly and " +
          "should only be used for debugging" )
    @Test
    public void shouldNotBeAbleToCloseTheSameStatementContextTwice() throws Exception
    {
        // GIVEN
        StatementState first = refCountingContext.newStatementState();
        refCountingContext.newStatementState();
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
            assertEqualsStatementClosed( e );
        }
        verify( actualState, never() ).close();
    }

    private void assertEqualsStatementClosed( IllegalStateException e )
    {
        assertEquals( "This " + StatementState.class.getSimpleName() +
                " has been closed. No more interaction allowed", e.getMessage() );
    }

    @Test
    public void shouldCloseAllStatementContextsOnCommit() throws Exception
    {
        // given
        refCountingContext.newStatementState();

        // when
        refCountingContext.commit();

        // then
        verify( actualState, times( 1 ) ).close();
    }

    @Test
    public void shouldCloseAllStatementContextsOnRollback() throws Exception
    {
        // given
        refCountingContext.newStatementState();

        // when
        refCountingContext.rollback();

        // then
        verify( actualState, times( 1 ) ).close();
    }

    @Test
    public void shouldBeAbleToOpenNewStatementContextsAfterCommit() throws Exception
    {
        // given
        refCountingContext.newStatementState();

        // when
        refCountingContext.commit();

        // then
        StatementState after = refCountingContext.newStatementState();
        verify( actualState, times( 1 ) ).close();
        verifyZeroInteractions( otherActualState );
        after.close();
        verify( otherActualState, times( 1 ) ).close();
    }
}
