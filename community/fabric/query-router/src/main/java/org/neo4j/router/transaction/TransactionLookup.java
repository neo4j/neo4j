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
package org.neo4j.router.transaction;

import java.util.Map;
import java.util.Optional;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.router.impl.transaction.RouterTransactionManager;

/**
 * A utility to look up either a Query Router or Composite (Fabric) transaction.
 */
public class TransactionLookup {

    private final RouterTransactionManager routerTransactionManager;
    private final TransactionManager maybeCompositeTransactionManager;

    public TransactionLookup(
            RouterTransactionManager routerTransactionManager, TransactionManager maybeCompositeTransactionManager) {
        this.routerTransactionManager = routerTransactionManager;
        this.maybeCompositeTransactionManager = maybeCompositeTransactionManager;
    }

    public Optional<Transaction> findTransactionContaining(InternalTransaction transaction) {
        Optional<RouterTransaction> routerTransaction = routerTransactionManager.findTransactionContaining(transaction);
        if (routerTransaction.isPresent()) {
            return routerTransaction.map(tx -> tx::setMetaData);
        }

        if (maybeCompositeTransactionManager != null) {
            return maybeCompositeTransactionManager
                    .findTransactionContaining(transaction)
                    .map(tx -> tx::setMetaData);
        }

        return Optional.empty();
    }

    public interface Transaction {

        void setMetaData(Map<String, Object> txMeta);
    }
}
