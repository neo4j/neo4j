/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitorScheduler;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

class KernelTransactionTimeoutMonitorSchedulerTest {

    private final KernelTransactionMonitor transactionMonitor = mock(KernelTransactionMonitor.class);
    private final JobScheduler jobScheduler = mock(JobScheduler.class);

    @Test
    void startJobTransactionMonitor() {
        JobHandle jobHandle = Mockito.mock(JobHandle.class);
        when(jobScheduler.scheduleRecurring(
                        eq(Group.TRANSACTION_TIMEOUT_MONITOR),
                        any(JobMonitoringParams.class),
                        eq(transactionMonitor),
                        anyLong(),
                        any(TimeUnit.class)))
                .thenReturn(jobHandle);

        TransactionMonitorScheduler monitorScheduler =
                new TransactionMonitorScheduler(transactionMonitor, jobScheduler, 7, "test database");

        monitorScheduler.start();
        verify(jobScheduler)
                .scheduleRecurring(
                        eq(Group.TRANSACTION_TIMEOUT_MONITOR),
                        any(JobMonitoringParams.class),
                        eq(transactionMonitor),
                        eq(7L),
                        eq(TimeUnit.MILLISECONDS));

        monitorScheduler.stop();
        verify(jobHandle).cancel();
    }
}
