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
package org.neo4j.internal.recordstorage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.lock.LockGroup;
import org.neo4j.storageengine.api.CommandStream;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.util.IdUpdateListener;

/**
 * Serves as executor of transactions, i.e. the visit... methods and will invoke the other lifecycle methods like {@link
 * TransactionApplierFactory#startTx(StorageEngineTransaction, BatchContext)}, {@link TransactionApplier#close()} ()} a.s.o
 * correctly.
 */
public class CommandHandlerContract {
    private CommandHandlerContract() {}

    @FunctionalInterface
    public interface ApplyFunction {
        boolean apply(TransactionApplier applier) throws Exception;
    }

    /**
     * Simply calls through to the {@link CommandStream#accept(Visitor)} method for each {@link
     * StorageEngineTransaction} given. This assumes that the {@link TransactionApplierFactory} will return {@link
     * TransactionApplier}s which actually do the work and that the transaction has all the relevant data.
     *
     * @param applier to use
     * @param transactions to apply
     */
    public static void apply(TransactionApplierFactory applier, StorageEngineTransaction... transactions)
            throws Exception {
        var batchContext = mock(BatchContext.class);
        when(batchContext.getLockGroup()).thenReturn(new LockGroup());
        when(batchContext.getIdUpdateListener()).thenReturn(IdUpdateListener.IGNORE);
        when(batchContext.getIndexActivator()).thenReturn(mock(IndexActivator.class));
        for (StorageEngineTransaction tx : transactions) {
            try (TransactionApplier txApplier = applier.startTx(tx, batchContext)) {
                tx.commandBatch().accept(txApplier);
            }
        }
    }

    /**
     * In case the transactions do not have the commands to apply, use this method to apply any commands you want with a
     * given {@link ApplyFunction} instead.
     *
     * @param applier to use
     * @param function which knows what to do with the {@link TransactionApplier}.
     * @param transactions are only used to create {@link TransactionApplier}s. The actual work is delegated to the
     * function.
     * @return the boolean-and result of all function operations.
     */
    public static boolean apply(
            TransactionApplierFactory applier, ApplyFunction function, StorageEngineTransaction... transactions)
            throws Exception {
        BatchContext batchContext = mock(BatchContext.class);
        when(batchContext.getLockGroup()).thenReturn(new LockGroup());
        when(batchContext.getIdUpdateListener()).thenReturn(IdUpdateListener.DIRECT);
        when(batchContext.getIndexActivator()).thenReturn(new IndexActivator(mock(IndexUpdateListener.class)));
        return apply(applier, function, batchContext, transactions);
    }

    /**
     * In case the transactions do not have the commands to apply, use this method to apply any commands you want with a
     * given {@link ApplyFunction} instead.
     *
     * @param applier to use
     * @param function which knows what to do with the {@link TransactionApplier}.
     * @param batchContext batch execution context
     * @param transactions are only used to create {@link TransactionApplier}s. The actual work is delegated to the
     * function.
     * @return the boolean-and result of all function operations.
     */
    public static boolean apply(
            TransactionApplierFactory applier,
            ApplyFunction function,
            BatchContext batchContext,
            StorageEngineTransaction... transactions)
            throws Exception {
        boolean result = true;
        for (StorageEngineTransaction tx : transactions) {
            try (TransactionApplier txApplier = applier.startTx(tx, batchContext)) {
                result &= function.apply(txApplier);
            }
        }
        return result;
    }
}
