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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DbmsExtension
class TransactionMemoryTrackerIT
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void failToAllocateOnClosedTransaction() throws TransactionFailureException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            kernelTransaction.closeTransaction();

            MemoryTracker memoryTracker = kernelTransaction.memoryTracker();
            assertThrows( AssertionError.class, () -> memoryTracker.allocateHeap( 1 ) );
            assertThrows( AssertionError.class, () -> memoryTracker.allocateNative( 1 ) );
        }
    }

    @Test
    void allocateOnOpenTransaction()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();

            MemoryTracker memoryTracker = kernelTransaction.memoryTracker();
            assertDoesNotThrow( () -> memoryTracker.allocateHeap( 1 ) );
            assertDoesNotThrow( () -> memoryTracker.allocateNative( 1 ) );
            assertDoesNotThrow( () -> memoryTracker.releaseNative( 1 ) );
            assertDoesNotThrow( () -> memoryTracker.releaseHeap( 1 ) );
        }
    }
}
