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
package org.neo4j.kernel;

import java.time.Clock;

import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.kernel.AvailabilityGuard.AvailabilityRequirement;
import static org.neo4j.kernel.AvailabilityGuard.availabilityRequirement;

/**
 * This class handles whether the database as a whole is available to use at all.
 * As it runs as the last service in the lifecycle list, the stop() is called first
 * on stop, shutdown or restart, and thus blocks access to everything else for outsiders.
 */
public class DatabaseAvailability implements Lifecycle
{
    private static final AvailabilityRequirement AVAILABILITY_REQUIREMENT = availabilityRequirement( "Database available" );
    private final AvailabilityGuard availabilityGuard;
    private final TransactionStats transactionMonitor;
    private final long awaitActiveTransactionDeadlineMillis;

    public DatabaseAvailability( AvailabilityGuard availabilityGuard, TransactionStats transactionMonitor,
            long awaitActiveTransactionDeadlineMillis )
    {
        this.availabilityGuard = availabilityGuard;
        this.transactionMonitor = transactionMonitor;
        this.awaitActiveTransactionDeadlineMillis = awaitActiveTransactionDeadlineMillis;

        // On initial setup, deny availability
        availabilityGuard.require( AVAILABILITY_REQUIREMENT );
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        availabilityGuard.fulfill( AVAILABILITY_REQUIREMENT );
    }

    @Override
    public void stop()
    {
        // Database is no longer available for use
        // Deny beginning new transactions
        availabilityGuard.require( AVAILABILITY_REQUIREMENT );

        // Await transactions stopped
        awaitTransactionsClosedWithinTimeout();
    }

    private void awaitTransactionsClosedWithinTimeout()
    {
        Clock clock = Clocks.systemClock();
        long deadline = clock.millis() + awaitActiveTransactionDeadlineMillis;
        while ( transactionMonitor.getNumberOfActiveTransactions() > 0 && clock.millis() < deadline )
        {
            parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
    }

    @Override
    public void shutdown()
    {
        // TODO: Starting database. Make sure none can access it through lock or CAS
    }
}
