/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Collections;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.test.Race;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DatabaseLinkedTransactionsHandlerTest
{
    KernelTransactions kernelTransactions = mock( KernelTransactions.class );

    @Disabled( "Handling of linked transactions is temporarily disabled" )
    @RepeatedTest( 100 )
    void testRaceConditionRegisterTerminate() throws Throwable
    {
        KernelTransactionHandle innerHandle = mock( KernelTransactionHandle.class );
        long innerTransactionId = 42;
        long outerTransactionId = 5;
        when( innerHandle.getUserTransactionId() ).thenReturn( innerTransactionId );
        when( kernelTransactions.executingTransactions() ).thenReturn( Collections.singleton( innerHandle ) );

        DatabaseLinkedTransactionsHandler terminationHandler = new DatabaseLinkedTransactionsHandler( kernelTransactions );

        Race race = new Race();
        race.addContestant( () -> terminationHandler.registerInnerTransaction( innerTransactionId, outerTransactionId ) );
        race.addContestants( 3, () -> terminationHandler.beforeTerminate( outerTransactionId, Status.Transaction.Terminated ) );
        race.shuffleContestants();
        race.go();

        verify( innerHandle, atLeastOnce() ).markForTermination( any() );
    }
}
