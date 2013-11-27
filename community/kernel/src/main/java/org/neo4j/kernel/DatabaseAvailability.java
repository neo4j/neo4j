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
package org.neo4j.kernel;

import javax.transaction.TransactionManager;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * This class handles whether the database as a whole is available to use at all.
 * As it runs as the last service in the lifecycle list, the stop() is called first
 * on stop, shutdown or restart, and thus blocks access to everything else for outsiders.
 */
public class DatabaseAvailability
        implements Lifecycle, AvailabilityGuard.AvailabilityRequirement
{
    private TransactionManager txManager;
    private AvailabilityGuard availabilityGuard;

    public DatabaseAvailability( TransactionManager txManager, AvailabilityGuard availabilityGuard )
    {
        this.txManager = txManager;
        this.availabilityGuard = availabilityGuard;
    }

    @Override
    public void init()
            throws Throwable
    {
    }

    @Override
    public void start()
            throws Throwable
    {
        availabilityGuard.grant(this);
    }

    @Override
    public void stop()
            throws Throwable
    {
        // Database is no longer available for use
        // Deny beginning new transactions
        availabilityGuard.deny(this);

        // If possible, wait until current transactions finish before continuing the shutdown
        if ( txManager instanceof TxManager )
        {
            // TODO make stop-deadline configurable
            long deadline = Clock.SYSTEM_CLOCK.currentTimeMillis() + 20 * 1000;

            TxManager realTxManager = (TxManager) txManager;
            while ( realTxManager.getActiveTxCount() > 0 && Clock.SYSTEM_CLOCK.currentTimeMillis() < deadline)
            {
                Thread.yield();
            }
        }
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        // TODO: Starting database. Make sure none can access it through lock or CAS
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }
}
