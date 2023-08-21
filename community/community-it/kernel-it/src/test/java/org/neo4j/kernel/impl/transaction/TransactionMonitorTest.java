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
package org.neo4j.kernel.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
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

    @Test
    void shouldHandleEffectiveNoOpWrite() {
        final var checker = checker();
        ExpectedDifference.NONE.verifyWith(checker);

        try (var tx = db.beginTx()) {
            ExpectedDifference.NONE.withStarted(1).withActive(1).verifyWith(checker);
            tx.createNode().delete();

            assertThat(hasTxStateWithChanges(tx))
                    .as(shouldHaveTxStateWithChanges(true))
                    .isTrue();

            ExpectedDifference.NONE.withStarted(1).withActive(1).isWriteTx(true).verifyWith(checker);
            tx.commit();
        }

        ExpectedDifference.NONE.withStarted(1).withCommitted(1).isWriteTx(true).verifyWith(checker);
    }

    @ParameterizedTest(name = "terminate on fail: {argumentsWithNames}")
    @ValueSource(booleans = {false, true})
    void shouldHandleErrorOnWrite(boolean terminate) {
        // create a node in one transaction
        // try deleting it from two different concurrent transactions
        // the metrics should still reflect they were both write transactions, despite one of them failing with
        // a tx state and no changes

        final String nodeId; // using id for simplicity; however, could be found through other means by user
        {
            final var checker = checker();
            ExpectedDifference.NONE.verifyWith(checker);

            try (var tx = db.beginTx()) {
                nodeId = tx.createNode().getElementId();
                tx.commit();
            }

            ExpectedDifference.NONE
                    .withStarted(1)
                    .withCommitted(1)
                    .isWriteTx(true)
                    .verifyWith(checker);
        }

        final var checker = checker();
        ExpectedDifference.NONE.verifyWith(checker);

        try (var successfulTx = db.beginTx();
                var unsuccessfulTx = db.beginTx()) {
            ExpectedDifference.NONE.withStarted(2).withActive(2).verifyWith(checker);

            // find the same node
            final var nodeToSuccessfullyDelete = successfulTx.getNodeByElementId(nodeId);
            final var nodeToUnsuccessfullyDelete = unsuccessfulTx.getNodeByElementId(nodeId);

            // delete node from one transaction
            nodeToSuccessfullyDelete.delete();
            assertThat(hasTxStateWithChanges(successfulTx))
                    .as(shouldHaveTxStateWithChanges(true))
                    .isTrue();
            successfulTx.commit();

            try {
                // try to delete node from the other transaction
                assertThatThrownBy(nodeToUnsuccessfullyDelete::delete, "node should be deleted")
                        .asInstanceOf(RethrowableThrowableAssert.factory(NotFoundException.class))
                        .hasMessageContainingAll("Unable to delete Node", "since it has already been deleted")
                        .rethrow(); // rethrow NotFoundException as it would throw here in user code
            } catch (NotFoundException error) {
                // catch here for checking
                ExpectedDifference.NONE
                        .withStarted(2)
                        .withActive(1)
                        .withCommitted(1)
                        .isWriteTx(true)
                        .verifyWith(checker);

                if (terminate) {
                    // could be some long-running cleanup that terminates via timeout
                    // or explicit termination like this
                    unsuccessfulTx.terminate();
                } else {
                    throw error; // rethrow to be consistent with normal user flow
                }

                unsuccessfulTx.commit(); // will not commit, will roll back, but still counts as write tx
            }
        } catch (NotFoundException ignored) {
            // explicitly expected to throw for test
            // perhaps user does something here in normal flow
        } catch (RuntimeException e) {
            assertThat(e)
                    .as("transaction failure exception")
                    .hasRootCauseInstanceOf(TransactionTerminatedException.class);
        }

        ExpectedDifference.NONE
                .withStarted(2)
                .withCommitted(1)
                .withRolledBack(1)
                .withTerminated(terminate ? 1 : 0)
                .isWriteTx(true)
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

    private static class RethrowableThrowableAssert<T extends Throwable>
            extends AbstractThrowableAssert<RethrowableThrowableAssert<T>, T> {
        static <T extends Throwable> InstanceOfAssertFactory<T, RethrowableThrowableAssert<T>> factory(
                Class<T> throwable) {
            return new InstanceOfAssertFactory<>(throwable, RethrowableThrowableAssert::new);
        }

        RethrowableThrowableAssert(T throwable) {
            super(throwable, RethrowableThrowableAssert.class);
        }

        void rethrow() throws T {
            throw actual;
        }
    }
}
