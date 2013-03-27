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
package org.neo4j.server.rest.transactional;

import static junit.framework.Assert.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

import org.junit.Test;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.server.rest.paging.FakeClock;
import org.neo4j.server.rest.transactional.error.InvalidTransactionIdError;

public class TimeoutEvictingTransactionRegistryTest
{

    @Test
    public void shouldSuspendTransationWhenPut() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TimeoutEvictingTransactionRegistry registry = new TimeoutEvictingTransactionRegistry( new FakeClock(), log);
        TransitionalTxManagementTransactionContext ctx = mock(TransitionalTxManagementTransactionContext.class);

        // When
        registry.put( 1337l, ctx );

        // Then
        verify( ctx ).suspendSinceTransactionsAreStillThreadBound();
    }

    @Test
    public void shouldSuspendAndResumeTransationWhenPutAndPulled() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TimeoutEvictingTransactionRegistry registry = new TimeoutEvictingTransactionRegistry( new FakeClock(), log );
        TransitionalTxManagementTransactionContext ctx = mock(TransitionalTxManagementTransactionContext.class);

        // When
        registry.put( 1337l, ctx );
        registry.pop( 1337l );

        // Then
        verify( ctx ).suspendSinceTransactionsAreStillThreadBound();
        verify( ctx ).resumeSinceTransactionsAreStillThreadBound();
    }

    @Test
    public void poppingATxTwiceShouldLeadToNoSuchTransactionError() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TimeoutEvictingTransactionRegistry registry = new TimeoutEvictingTransactionRegistry( new FakeClock(), log );
        TransitionalTxManagementTransactionContext ctx = mock(TransitionalTxManagementTransactionContext.class);

        // And Given
        registry.put( 1337l, ctx );
        registry.pop( 1337l );

        // When
        try {
            registry.pop( 1337l );
            fail("Should have thrown exception");
        } catch(InvalidTransactionIdError exc)
        {
            // ok
        }
    }

    @Test
    public void poppingANonExistantTxShouldThrowError() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TimeoutEvictingTransactionRegistry registry = new TimeoutEvictingTransactionRegistry( new FakeClock(), log );

        // When
        try {
            registry.pop( 1337l );
            fail("Should have thrown exception");
        } catch(InvalidTransactionIdError exc)
        {
            // ok
        }
    }

    @Test
    public void transactionsShouldBeEvictedWhenUnusedLongerThanTimeout() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        TestLogger log = new TestLogger();
        TimeoutEvictingTransactionRegistry registry = new TimeoutEvictingTransactionRegistry( clock, log );
        TransactionContext oldTx = mock( TransitionalTxManagementTransactionContext.class );
        TransactionContext newTx = mock( TransitionalTxManagementTransactionContext.class );

        // And given one transaction was stored one minute ago, and another was stored just now
        registry.put( 1l, oldTx );
        clock.forwardMinutes( 1 );
        registry.put( 2l, newTx );

        // When
        registry.evictAllIdleSince( clock.currentTimeInMilliseconds() - 1000 );

        // Then
        assertThat( registry.pop( 2l ), equalTo( newTx ) );

        // And then the other should have been evicted
        try {
            registry.pop( 1l );
            fail( "Should have thrown exception" );
        } catch(InvalidTransactionIdError exc)
        {
            // ok
        }

        log.assertExactly( info( "Transaction with id 1 has been idle for 60 seconds, and has been automatically rolled back." ) );
    }
}
