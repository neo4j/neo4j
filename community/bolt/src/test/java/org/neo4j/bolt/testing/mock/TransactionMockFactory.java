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

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.values.virtual.MapValue;

public class TransactionMockFactory extends AbstractMockFactory<Transaction, TransactionMockFactory> {
    private static final String DEFAULT_ID = "tx-test";

    private TransactionMockFactory(String id) {
        super(Transaction.class);

        var idPool = new AtomicLong();
        this.withId(id)
                .withTransactionType(TransactionType.EXPLICIT)
                .withOpenCaptor()
                .withCloseCaptor()
                .withFactory((statement, params) -> StatementMockFactory.newInstance(idPool.getAndIncrement()));
    }

    public static TransactionMockFactory newFactory(String id) {
        return new TransactionMockFactory(id);
    }

    public static TransactionMockFactory newFactory() {
        return newFactory(DEFAULT_ID);
    }

    public static Transaction newInstance(String id) {
        return newFactory(id).build();
    }

    public static Transaction newInstance() {
        return newFactory().build();
    }

    public TransactionMockFactory withId(String id) {
        return this.withStaticValue(Transaction::id, id);
    }

    public TransactionMockFactory withTransactionType(TransactionType type) {
        return this.withStaticValue(Transaction::type, type);
    }

    public TransactionMockFactory withOpen(boolean open) {
        return this.withStaticValue(Transaction::isOpen, open);
    }

    public TransactionMockFactory withValid(boolean valid) {
        return this.withStaticValue(Transaction::isValid, valid);
    }

    public TransactionMockFactory withLatestStatementId(long statementId) {
        return this.withStaticValue(Transaction::latestStatementId, statementId);
    }

    public TransactionMockFactory withOpenStatement(boolean openStatement) {
        return this.withStaticValue(Transaction::hasOpenStatement, openStatement);
    }

    public TransactionMockFactory withFactory(StatementFactory factory) {
        var statementMap = new HashMap<Long, Statement>();
        var latestStatementId = new AtomicLong();

        return this.withAnswer(
                        it -> {
                            try {
                                it.run(Mockito.any(), Mockito.any());
                            } catch (StatementException ignore) {
                                // stubbing call
                            }
                        },
                        invocation -> {
                            var statement = factory.create(invocation.getArgument(0), invocation.getArgument(1));

                            var id = statement.id();

                            statementMap.put(id, statement);
                            latestStatementId.set(id);

                            // if the returned object is a mock, we consume the id call so that it does
                            // break verification calls within the caller context
                            if (MockUtil.isMock(statement)) {
                                Mockito.verify(statement).id();
                            }

                            return statement;
                        })
                .withAnswer(Transaction::latestStatementId, invocation -> latestStatementId.get())
                .withAnswer(
                        it -> it.getStatement(Mockito.anyLong()),
                        invocation -> Optional.ofNullable(statementMap.get(invocation.getArgument(0))));
    }

    public TransactionMockFactory withBookmark(String bookmark) {
        return this.withStaticValue(
                it -> {
                    try {
                        return it.commit();
                    } catch (TransactionException ignore) {
                        // stubbing call
                        return null;
                    }
                },
                bookmark);
    }

    public TransactionMockFactory withOpenCaptor(AtomicBoolean captor) {
        return this.withAnswer(Transaction::isOpen, invocation -> captor.get())
                .with(it -> {
                    try {
                        Mockito.doAnswer(invocation -> {
                                    captor.set(false);
                                    return "some-bookmark-1234";
                                })
                                .when(it)
                                .commit();
                    } catch (TransactionException ignore) {
                        // stubbing call
                    }
                })
                .with(it -> {
                    try {
                        Mockito.doAnswer(invocation -> {
                                    captor.set(false);
                                    return null;
                                })
                                .when(it)
                                .rollback();
                    } catch (TransactionException ignore) {
                        // stubbing call
                    }
                });
    }

    public TransactionMockFactory withOpenCaptor() {
        return this.withOpenCaptor(new AtomicBoolean(true));
    }

    public TransactionMockFactory withOpenCaptor(AtomicBoolean captor, String bookmark) {
        return this.withOpenCaptor(captor).with(it -> {
            try {
                Mockito.doAnswer(invocation -> {
                            captor.set(false);
                            return bookmark;
                        })
                        .when(it)
                        .commit();
            } catch (TransactionException ignore) {
                // stubbing call
            }
        });
    }

    public TransactionMockFactory withOpenCaptor(String bookmark) {
        return this.withOpenCaptor(new AtomicBoolean(false), bookmark);
    }

    public TransactionMockFactory withInterruptCaptor(AtomicBoolean captor) {
        return this.withAnswer(Transaction::interrupt, invocation -> {
            captor.set(true);
            return null;
        });
    }

    public TransactionMockFactory withValidationResult(boolean validationResult) {
        return this.with(it -> Mockito.doReturn(validationResult).when(it).validate());
    }

    public TransactionMockFactory withCloseCaptor(AtomicBoolean captor) {
        return this.withAnswer(Transaction::isValid, invocation -> captor.get())
                .withAnswer(
                        it -> {
                            try {
                                it.close();
                            } catch (TransactionException ignore) {
                                // stubbing call
                            }
                        },
                        invocation -> {
                            captor.set(false);
                            return null;
                        });
    }

    public TransactionMockFactory withCloseCaptor() {
        return this.withCloseCaptor(new AtomicBoolean(true));
    }

    @FunctionalInterface
    public interface StatementFactory {

        Statement create(String statement, MapValue params) throws StatementException;
    }
}
