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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
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
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TransactionStateMachineV1SPITest
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

        DatabaseAvailabilityGuard databaseAvailabilityGuard = spy( new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, clock, NullLog.getInstance() ) );
        when( databaseAvailabilityGuard.isAvailable() ).then( invocation ->
        {
            // move clock forward on the first availability check
            // this check is executed on every tx id polling iteration
            boolean available = (boolean) invocation.callRealMethod();
            clock.forward( txAwaitDuration.getSeconds() + 1, SECONDS );
            return available;
        } );

        TransactionStateMachineV1SPI txSpi = createTxSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );

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

        TransactionStateMachineV1SPI txSpi = createTxSpi( txIdStore, Duration.ZERO, Clock.systemUTC() );

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

    private static TransactionStateMachineV1SPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration,
            Clock clock )
    {
        DatabaseAvailabilityGuard databaseAvailabilityGuard = new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, clock, NullLog.getInstance() );
        return createTxSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );
    }

    private static TransactionStateMachineV1SPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration,
            DatabaseAvailabilityGuard availabilityGuard, Clock clock )
    {
        QueryExecutionEngine queryExecutionEngine = mock( QueryExecutionEngine.class );

        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge( availabilityGuard );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( bridge );
        when( dependencyResolver.resolveDependency( QueryExecutionEngine.class ) ).thenReturn( queryExecutionEngine );
        when( dependencyResolver.resolveDependency( DatabaseAvailabilityGuard.class ) ).thenReturn( availabilityGuard );
        when( dependencyResolver.provideDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );

        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );

        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );

        BoltChannel boltChannel = new BoltChannel( "bolt-42", "bolt", new EmbeddedChannel() );

        return new TransactionStateMachineV1SPI( db, boltChannel, txAwaitDuration, clock );
    }
}
