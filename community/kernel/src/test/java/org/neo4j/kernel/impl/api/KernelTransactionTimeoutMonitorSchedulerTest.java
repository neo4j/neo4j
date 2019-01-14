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
package org.neo4j.kernel.impl.api;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.JobScheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.scheduler.JobScheduler.Groups.transactionTimeoutMonitor;

public class KernelTransactionTimeoutMonitorSchedulerTest
{

    private final KernelTransactionTimeoutMonitor transactionMonitor = mock( KernelTransactionTimeoutMonitor.class );
    private final JobScheduler jobScheduler = mock( JobScheduler.class );

    @Test
    public void startJobTransactionMonitor()
    {
        JobScheduler.JobHandle jobHandle = Mockito.mock( JobScheduler.JobHandle.class );
        when( jobScheduler.scheduleRecurring( eq(transactionTimeoutMonitor), eq( transactionMonitor), anyLong(),
                any(TimeUnit.class) )).thenReturn( jobHandle );

        KernelTransactionMonitorScheduler monitorScheduler =
                new KernelTransactionMonitorScheduler( transactionMonitor, jobScheduler, 7 );

        monitorScheduler.start();
        verify(jobScheduler).scheduleRecurring( transactionTimeoutMonitor, transactionMonitor,
                7, TimeUnit.MILLISECONDS );

        monitorScheduler.stop();
        verify( jobHandle ).cancel( true );
    }
}
