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
package org.neo4j.bolt.testing.mock;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;

public final class TransactionManagerMockFactory
        extends AbstractMockFactory<TransactionManager, TransactionManagerMockFactory> {

    private TransactionManagerMockFactory() {
        super(TransactionManager.class);

        // TODO: Remove closed transactions
        var idPool = new AtomicLong();
        this.withFactory((type, owner, databaseName, mode, bookmarks, timeout, metadata) ->
                TransactionMockFactory.newFactory("bolt-test-" + idPool.getAndIncrement())
                        .withTransactionType(type)
                        .build());
    }

    public static TransactionManagerMockFactory newFactory() {
        return new TransactionManagerMockFactory();
    }

    public static TransactionManager newInstance() {
        return newFactory().build();
    }

    public TransactionManagerMockFactory withFactory(TransactionFactory factory) {
        var transactionMap = new HashMap<String, Transaction>();

        return this.withAnswer(
                        it -> {
                            try {
                                it.create(
                                        Mockito.any(),
                                        Mockito.any(),
                                        Mockito.any(),
                                        Mockito.any(),
                                        Mockito.anyList(),
                                        Mockito.any(),
                                        Mockito.anyMap(),
                                        Mockito.any());
                            } catch (TransactionException ignore) {
                                // stubbing call
                            }
                        },
                        invocation -> {
                            var transaction = factory.create(
                                    invocation.getArgument(0),
                                    invocation.getArgument(1),
                                    invocation.getArgument(2),
                                    invocation.getArgument(3),
                                    invocation.getArgument(4),
                                    invocation.getArgument(5),
                                    invocation.getArgument(6));

                            transactionMap.put(transaction.id(), transaction);

                            // if the returned object is a mock, we consume the id call so that it does
                            // break verification calls within the caller context
                            if (MockUtil.isMock(transaction)) {
                                Mockito.verify(transaction).id();
                            }

                            return transaction;
                        })
                .withAnswer(it -> it.get(Mockito.anyString()), invocation -> {
                    var transactionId = invocation.<String>getArgument(0);

                    return Optional.ofNullable(transactionMap.get(transactionId));
                })
                .withAnswer(TransactionManager::getTransactionCount, invocation -> transactionMap.size());
    }

    @FunctionalInterface
    public interface TransactionFactory {

        Transaction create(
                TransactionType type,
                TransactionOwner owner,
                String databaseName,
                AccessMode mode,
                List<String> bookmarks,
                Duration timeout,
                Map<String, Object> metadata)
                throws TransactionException;
    }
}
