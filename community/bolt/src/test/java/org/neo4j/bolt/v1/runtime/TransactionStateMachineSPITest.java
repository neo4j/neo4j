/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TransactionStateMachineSPITest
{
    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test
    public void throwsWhenTxAwaitDurationExpires()
    {
        long lastClosedTransactionId = 100;
        Supplier<TransactionIdStore> txIdStore = () -> fixedTxIdStore( lastClosedTransactionId );
        Duration txAwaitDuration = Duration.ofSeconds( 42 );
        FakeClock clock = new FakeClock();

        AvailabilityGuard availabilityGuard = spy( new AvailabilityGuard( clock, NullLog.getInstance() ) );
        when( availabilityGuard.isAvailable() ).then( invocation ->
        {
            // move clock forward on the first availability check
            // this check is executed on every tx id polling iteration
            boolean available = (boolean) invocation.callRealMethod();
            clock.forward( txAwaitDuration.getSeconds() + 1, SECONDS );
            return available;
        } );

        TransactionStateMachineSPI txSpi = createTxSpi( txIdStore, txAwaitDuration, availabilityGuard, clock );

        Future<Void> result = otherThread.execute( state ->
        {
            txSpi.awaitUpToDate( lastClosedTransactionId + 42 );
            return null;
        } );

        try
        {
            result.get( 20, SECONDS );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( TransactionFailureException.class ) );
        }
    }

    @Test
    public void doesNotWaitWhenTxIdUpToDate() throws Exception
    {
        long lastClosedTransactionId = 100;
        Supplier<TransactionIdStore> txIdStore = () -> fixedTxIdStore( lastClosedTransactionId );

        TransactionStateMachineSPI txSpi = createTxSpi( txIdStore, Duration.ZERO, Clock.systemUTC() );

        Future<Void> result = otherThread.execute( state ->
        {
            txSpi.awaitUpToDate( lastClosedTransactionId - 42 );
            return null;
        } );

        assertNull( result.get( 20, SECONDS ) );
    }

    private static TransactionIdStore fixedTxIdStore( long lastClosedTransactionId )
    {
        TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( lastClosedTransactionId );
        return txIdStore;
    }

    private static TransactionStateMachineSPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration,
            Clock clock )
    {
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, NullLog.getInstance() );
        return createTxSpi( txIdStore, txAwaitDuration, availabilityGuard, clock );
    }

    private static TransactionStateMachineSPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration,
            AvailabilityGuard availabilityGuard, Clock clock )
    {
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );

        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );

        when(dependencyResolver.provideDependency( TransactionIdStore.class )).thenReturn( txIdStore );

        return new TransactionStateMachineSPI( db, new ThreadToStatementContextBridge(),
                mock( QueryExecutionEngine.class ), availabilityGuard, queryService, txAwaitDuration,
                clock );
    }
}
