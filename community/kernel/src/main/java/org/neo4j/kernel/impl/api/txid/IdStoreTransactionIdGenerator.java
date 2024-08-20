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
package org.neo4j.kernel.impl.api.txid;

import static java.util.Objects.requireNonNull;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;

import org.neo4j.storageengine.api.TransactionIdStore;

public class IdStoreTransactionIdGenerator implements TransactionIdGenerator {

    private final TransactionIdStore idStore;

    public IdStoreTransactionIdGenerator(TransactionIdStore idStore) {
        this.idStore = requireNonNull(idStore);
    }

    @Override
    public long nextId(long externalId) {
        long txId = idStore.nextCommittingTransactionId();
        if (externalId != UNKNOWN_TX_ID) {
            validate(txId, externalId);
        }
        return txId;
    }

    private void validate(long transactionId, long expectedTxId) {
        if (transactionId != expectedTxId) {
            throw new IllegalStateException("Received commands batch with txId:" + expectedTxId
                    + " to be applied, but appending it ended up generating an unexpected txId:"
                    + transactionId);
        }
    }
}
