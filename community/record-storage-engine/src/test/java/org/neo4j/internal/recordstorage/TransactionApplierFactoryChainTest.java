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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;

class TransactionApplierFactoryChainTest {
    private TransactionApplierFactoryChain facade;
    private TransactionApplierFactory applier1;
    private TransactionApplierFactory applier2;
    private TransactionApplierFactory applier3;
    private TransactionApplier txApplier1;
    private TransactionApplier txApplier2;
    private TransactionApplier txApplier3;

    @BeforeEach
    void setUp() throws Exception {
        txApplier1 = mock(TransactionApplier.class);
        applier1 = mock(TransactionApplierFactory.class);
        when(applier1.startTx(any(StorageEngineTransaction.class), any(BatchContext.class)))
                .thenReturn(txApplier1);

        txApplier2 = mock(TransactionApplier.class);
        applier2 = mock(TransactionApplierFactory.class);
        when(applier2.startTx(any(StorageEngineTransaction.class), any(BatchContext.class)))
                .thenReturn(txApplier2);

        txApplier3 = mock(TransactionApplier.class);
        applier3 = mock(TransactionApplierFactory.class);
        when(applier3.startTx(any(StorageEngineTransaction.class), any(BatchContext.class)))
                .thenReturn(txApplier3);

        facade = new TransactionApplierFactoryChain(IdGeneratorUpdatesWorkSync::newBatch, applier1, applier2, applier3);
    }

    @Test
    void testStartTxCorrectOrder() throws Exception {
        // GIVEN
        var tx = mock(StorageEngineTransaction.class);
        var batchContext = mock(BatchContext.class);

        // WHEN
        TransactionApplierFacade result = (TransactionApplierFacade) facade.startTx(tx, batchContext);

        // THEN
        InOrder inOrder = inOrder(applier1, applier2, applier3);

        inOrder.verify(applier1).startTx(tx, batchContext);
        inOrder.verify(applier2).startTx(tx, batchContext);
        inOrder.verify(applier3).startTx(tx, batchContext);

        assertEquals(txApplier1, result.appliers[0]);
        assertEquals(txApplier2, result.appliers[1]);
        assertEquals(txApplier3, result.appliers[2]);
        assertEquals(3, result.appliers.length);
    }

    @Test
    void testStartTxCorrectOrderWithLockGroup() throws Exception {
        // GIVEN
        StorageEngineTransaction tx = mock(StorageEngineTransaction.class);
        var batchContext = mock(BatchContext.class);

        // WHEN
        TransactionApplierFacade result = (TransactionApplierFacade) facade.startTx(tx, batchContext);

        // THEN
        InOrder inOrder = inOrder(applier1, applier2, applier3);

        inOrder.verify(applier1).startTx(tx, batchContext);
        inOrder.verify(applier2).startTx(tx, batchContext);
        inOrder.verify(applier3).startTx(tx, batchContext);

        assertEquals(txApplier1, result.appliers[0]);
        assertEquals(txApplier2, result.appliers[1]);
        assertEquals(txApplier3, result.appliers[2]);
        assertEquals(3, result.appliers.length);
    }
}
