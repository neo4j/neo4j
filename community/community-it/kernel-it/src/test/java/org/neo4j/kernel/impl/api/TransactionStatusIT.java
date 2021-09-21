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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import org.neo4j.procedure.builtin.TransactionId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace.NONE;
import static org.neo4j.time.Clocks.nanoClock;

@DbmsExtension
public class TransactionStatusIT
{
    @Inject
    private GraphDatabaseAPI database;

    @Test
    void emptyInitializationTraceOfClosedTransaction()
    {
        var transaction = database.beginTransaction( EXPLICIT, AUTH_DISABLED );
        var kernelTransaction = transaction.kernelTransaction();
        transaction.close();
        var handle = new KernelTransactionImplementationHandle( (KernelTransactionImplementation) kernelTransaction, nanoClock() );
        assertSame( NONE, handle.transactionInitialisationTrace() );
    }

    @Test
    void closedTransactionEmptyConnectionsDetails()
    {
        var transaction = database.beginTransaction( EXPLICIT, AUTH_DISABLED );
        var kernelTransaction = transaction.kernelTransaction();
        transaction.close();
        var handle = new KernelTransactionImplementationHandle( (KernelTransactionImplementation) kernelTransaction, nanoClock() );
        assertFalse( handle.clientInfo().isPresent() );
    }

    @Test
    void closedTransactionEmptyQueryDetails()
    {
        var transaction = database.beginTransaction( EXPLICIT, AUTH_DISABLED );
        var kernelTransaction = transaction.kernelTransaction();
        transaction.close();
        var handle = new KernelTransactionImplementationHandle( (KernelTransactionImplementation) kernelTransaction, nanoClock() );
        assertDoesNotThrow( () -> new TransactionId( "test", handle.getUserTransactionId() ) );
    }
}
