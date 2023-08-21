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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.Race;

class InnerTransactionHandlerImplTest {

    KernelTransactions kernelTransactions = mock(KernelTransactions.class);

    // While #testRaceConditionRegisterTerminate() tests whether we can get in
    // trouble when racing two threads on markForTermination and
    // registerInnerTransaction while the next two tests do this in a
    // sequential way to more easily pin-point was is going wrong for simple
    // cases.

    @Test
    void testRaceConditionRegisterTerminate() throws Throwable {
        for (int i = 0; i < 100; i++) {
            KernelTransactionHandle innerHandle = mock(KernelTransactionHandle.class);
            long innerTransactionId = 42;
            when(innerHandle.getTransactionSequenceNumber()).thenReturn(innerTransactionId);
            when(kernelTransactions.executingTransactions()).thenReturn(Collections.singleton(innerHandle));

            InnerTransactionHandlerImpl terminationHandler = new InnerTransactionHandlerImpl(kernelTransactions);

            Race race = new Race();
            race.addContestant(() -> terminationHandler.registerInnerTransaction(innerTransactionId));
            race.addContestants(3, () -> terminationHandler.terminateInnerTransactions(Status.Transaction.Terminated));
            race.shuffleContestants();
            race.go();

            verify(innerHandle, atLeastOnce()).markForTermination(any());
        }
    }

    @Test
    void testRegisterInnerTransactionBeforeTerminate() {
        KernelTransactionHandle innerHandle = mock(KernelTransactionHandle.class);
        long innerTransactionId = 42;
        when(innerHandle.getTransactionSequenceNumber()).thenReturn(innerTransactionId);
        when(kernelTransactions.executingTransactions()).thenReturn(Collections.singleton(innerHandle));

        InnerTransactionHandlerImpl terminationHandler = new InnerTransactionHandlerImpl(kernelTransactions);
        terminationHandler.registerInnerTransaction(innerTransactionId);
        terminationHandler.terminateInnerTransactions(Status.Transaction.Terminated);

        verify(innerHandle, atLeastOnce()).markForTermination(any());
    }

    @Test
    void testRegisterAfterTerminate() {
        KernelTransactionHandle innerHandle = mock(KernelTransactionHandle.class);
        long innerTransactionId = 42;
        when(innerHandle.getTransactionSequenceNumber()).thenReturn(innerTransactionId);
        when(kernelTransactions.executingTransactions()).thenReturn(Collections.singleton(innerHandle));

        InnerTransactionHandlerImpl terminationHandler = new InnerTransactionHandlerImpl(kernelTransactions);
        terminationHandler.terminateInnerTransactions(Status.Transaction.Terminated);
        terminationHandler.registerInnerTransaction(innerTransactionId);

        verify(innerHandle, atLeastOnce()).markForTermination(any());
    }

    @Test
    void testRegisterWithoutClose() {
        KernelTransactionHandle innerHandle = mock(KernelTransactionHandle.class);
        long innerTransactionId = 42;
        when(innerHandle.getTransactionSequenceNumber()).thenReturn(innerTransactionId);
        when(kernelTransactions.executingTransactions()).thenReturn(Collections.singleton(innerHandle));

        InnerTransactionHandlerImpl terminationHandler = new InnerTransactionHandlerImpl(kernelTransactions);
        terminationHandler.registerInnerTransaction(innerTransactionId);

        verify(innerHandle, never()).markForTermination(any());
    }
}
