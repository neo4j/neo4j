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

public interface FabricRemoteExecutor {
    RemoteTransactionContext startTransactionContext(
            CompoundTransaction<SingleDbTransaction> compositeTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager);

    interface RemoteTransactionContext extends AutoCloseable {
        Mono<StatementResult> run(
                Location.Remote location,
                ExecutionOptions options,
                String query,
                TransactionMode transactionMode,
                MapValue params);

        /**
         * A method for executing CALL IN TRANSACTIONS.
         * The reason why CALL IN TRANSACTIONS has a special entry point is that a lot of
         * restrictions like being able to write to only one graph per Fabric transaction
         * don't apply.
         * <p>
         * Unlike the generic remote query execution, CALL IN TRANSACTIONS cannot be executed in parallel,
         * so this method tries to save some grief to the users and is synchronous.
         */
        StatementResult runInAutocommitTransaction(
                Location.Remote location,
                ExecutionOptions executionOptions,
                String query,
                TransactionMode transactionMode,
                MapValue params);

        boolean isEmptyContext();

        @Override
        void close();
    }
}
