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
package org.neo4j.fabric.executor;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.values.virtual.MapValue;
import reactor.core.publisher.Mono;

public class ThrowingFabricRemoteExecutor implements FabricRemoteExecutor {
    @Override
    public RemoteTransactionContext startTransactionContext(
            CompoundTransaction compositeTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager) {
        return new RemoteTransactionContextImpl();
    }

    private static class RemoteTransactionContextImpl implements RemoteTransactionContext {

        @Override
        public Mono<StatementResult> run(
                Location.Remote location,
                ExecutionOptions executionOptions,
                String query,
                TransactionMode transactionMode,
                MapValue params) {
            throw new IllegalStateException("Remote query execution not supported");
        }

        @Override
        public StatementResult runInAutocommitTransaction(
                Location.Remote location,
                ExecutionOptions executionOptions,
                String query,
                TransactionMode transactionMode,
                MapValue params) {
            throw new IllegalStateException("Remote query execution not supported");
        }

        @Override
        public boolean isEmptyContext() {
            return true;
        }

        @Override
        public void close() {}
    }
}
