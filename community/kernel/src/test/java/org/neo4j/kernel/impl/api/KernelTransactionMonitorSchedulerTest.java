/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitorScheduler;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class KernelTransactionMonitorSchedulerTest
{
    private final JobScheduler scheduler = mock( JobScheduler.class );
    private final KernelTransactionMonitor transactionTimeoutMonitor = mock( KernelTransactionMonitor.class );

    @Test
    void scheduleRecurringMonitorJobIfConfigured()
    {
        KernelTransactionMonitorScheduler transactionMonitorScheduler = createMonitorScheduler(1);
        transactionMonitorScheduler.start();

        verify( scheduler).scheduleRecurring( Group.TRANSACTION_TIMEOUT_MONITOR, transactionTimeoutMonitor, 1, TimeUnit
                .MILLISECONDS );
    }

    @Test
    void doNotScheduleMonitorJobIfDisabled()
    {
        KernelTransactionMonitorScheduler transactionMonitorScheduler = createMonitorScheduler( 0 );
        transactionMonitorScheduler.start();

        verifyNoInteractions( scheduler);
    }

    private KernelTransactionMonitorScheduler createMonitorScheduler( long checkInterval )
    {
        return new KernelTransactionMonitorScheduler( transactionTimeoutMonitor, scheduler, checkInterval );
    }
}
