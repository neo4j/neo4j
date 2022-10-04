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
package org.neo4j.kernel.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.transaction.TransactionCountersChecker.ExpectedDifference;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class TransactionMonitorTest {
    @Inject
    private GraphDatabaseAPI db;

    private TransactionCounters counts;

    @BeforeEach
    void setup() {
        counts = db.getDependencyResolver().resolveDependency(TransactionCounters.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void shouldCountCommittedTransactions(String name, boolean isWriteTx, Consumer<Transaction> txConsumer) {
        final var checker = checker();
        ExpectedDifference.NONE.verifyWith(checker);

        try (var tx = db.beginTx()) {
            ExpectedDifference.NONE.withStarted(1).withActive(1).verifyWith(checker);
            txConsumer.accept(tx);

            assertThat(hasTxStateWithChanges(tx))
                    .as(shouldHaveTxStateWithChanges(isWriteTx))
                    .isEqualTo(isWriteTx);

            ExpectedDifference.NONE
                    .withStarted(1)
                    .withActive(1)
                    .isWriteTx(isWriteTx)
                    .verifyWith(checker);

            tx.commit();
        }

        ExpectedDifference.NONE
                .withStarted(1)
                .isWriteTx(isWriteTx)
                .withCommitted(1)
                .verifyWith(checker);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void shouldCountRolledBackTransactions(String name, boolean isWriteTx, Consumer<Transaction> txConsumer) {
        final var checker = checker();
        ExpectedDifference.NONE.verifyWith(checker);

        try (var tx = db.beginTx()) {
            ExpectedDifference.NONE.withStarted(1).withActive(1).verifyWith(checker);
            txConsumer.accept(tx);

            assertThat(hasTxStateWithChanges(tx))
                    .as(shouldHaveTxStateWithChanges(isWriteTx))
                    .isEqualTo(isWriteTx);

            ExpectedDifference.NONE
                    .withStarted(1)
                    .withActive(1)
                    .isWriteTx(isWriteTx)
                    .verifyWith(checker);

            tx.rollback();
        }

        ExpectedDifference.NONE
                .withStarted(1)
                .withRolledBack(1)
                .isWriteTx(isWriteTx)
                .verifyWith(checker);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void shouldCountTerminatedTransactions(String name, boolean isWriteTx, Consumer<Transaction> txConsumer) {
        final var checker = checker();
        ExpectedDifference.NONE.verifyWith(checker);

        try (var tx = db.beginTx()) {
            ExpectedDifference.NONE.withStarted(1).withActive(1).verifyWith(checker);
            txConsumer.accept(tx);

            assertThat(hasTxStateWithChanges(tx))
                    .as(shouldHaveTxStateWithChanges(isWriteTx))
                    .isEqualTo(isWriteTx);

            ExpectedDifference.NONE
                    .withStarted(1)
                    .withActive(1)
                    .isWriteTx(isWriteTx)
                    .verifyWith(checker);

            tx.terminate();
        }

        ExpectedDifference.NONE
                .withStarted(1)
                .withRolledBack(1)
                .withTerminated(1)
                .isWriteTx(isWriteTx)
                .verifyWith(checker);
    }

    private TransactionCountersChecker checker() {
        return TransactionCountersChecker.checkerFor(counts);
    }

    private static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of("read", false, (Consumer<Transaction>) tx -> {}),
                Arguments.of("write", true, (Consumer<Transaction>) Transaction::createNode));
    }

    private static boolean hasTxStateWithChanges(Transaction tx) {
        return ((KernelTransactionImplementation) ((InternalTransaction) tx).kernelTransaction())
                .hasTxStateWithChanges();
    }

    private static Description shouldHaveTxStateWithChanges(boolean isWriteTx) {
        final var type = isWriteTx ? "write" : "read";
        final var negation = isWriteTx ? "" : " not";
        return new TextDescription("%s transaction should%s have state with changes", type, negation);
    }
}
