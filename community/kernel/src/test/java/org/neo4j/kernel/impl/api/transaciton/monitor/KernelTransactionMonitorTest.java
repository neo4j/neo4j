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
package org.neo4j.kernel.impl.api.transaciton.monitor;

import org.junit.jupiter.api.Test;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KernelTransactionMonitorTest
{
    @Test
    void shouldNotTimeoutSchemaTransactions()
    {
        // given
        KernelTransactions kernelTransactions = mock( KernelTransactions.class );
        FakeClock clock = new FakeClock( 100, MINUTES );
        KernelTransactionMonitor monitor = new KernelTransactionMonitor( kernelTransactions, clock, NullLogService.getInstance() );
        // a 2 minutes old schema transaction which has a timeout of 1 minute
        KernelTransactionHandle oldSchemaTransaction = mock( KernelTransactionHandle.class );
        when( oldSchemaTransaction.isSchemaTransaction() ).thenReturn( true );
        when( oldSchemaTransaction.startTime() ).thenReturn( clock.millis() - MINUTES.toMillis( 2 ) );
        when( oldSchemaTransaction.timeoutMillis() ).thenReturn( MINUTES.toMillis( 1 ) );
        when( kernelTransactions.activeTransactions() ).thenReturn( Iterators.asSet( oldSchemaTransaction ) );

        // when
        monitor.run();

        // then
        verify( oldSchemaTransaction, times( 1 ) ).isSchemaTransaction();
        verify( oldSchemaTransaction, never() ).markForTermination( any() );
    }
}
