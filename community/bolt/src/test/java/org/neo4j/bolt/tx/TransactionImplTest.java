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
package org.neo4j.bolt.tx;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.tx.error.TransactionCloseException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.time.FakeClock;
import org.neo4j.values.virtual.MapValue;

class TransactionImplTest {

    private DatabaseReference databaseReference;
    private Clock clock;
    private BoltTransaction boltTransaction;
    private final String bookmark = "some-bookmark";

    @BeforeEach
    void prepare() {
        this.databaseReference = Mockito.mock(DatabaseReference.class);
        this.clock = FakeClock.fixed(Instant.EPOCH, ZoneOffset.UTC);
        this.boltTransaction = Mockito.mock(BoltTransaction.class, Mockito.RETURNS_MOCKS);

        Mockito.doReturn(this.bookmark).when(this.boltTransaction).getBookmark();
    }

    @Test
    void shouldReturnGivenId() {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.id()).isEqualTo("bolt-42");
    }

    @Test
    void shouldReturnGivenType() {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.type()).isEqualTo(TransactionType.EXPLICIT);
    }

    @Test
    void shouldIndicateInitialState() {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.isOpen()).isTrue();
        Assertions.assertThat(transaction.isValid()).isTrue();
        Assertions.assertThat(transaction.latestStatementId()).isEqualTo(0);
        Assertions.assertThat(transaction.hasOpenStatement()).isFalse();
    }

    @Test
    void shouldManageStatements() throws QueryExecutionKernelException, StatementException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.hasOpenStatement()).isFalse();

        var statement = transaction.run("ACTUAL REAL QUERY THAT DOES STUFF", MapValue.EMPTY);

        var inOrder = Mockito.inOrder(this.boltTransaction);

        inOrder.verify(this.boltTransaction)
                .executeQuery(
                        Mockito.eq("ACTUAL REAL QUERY THAT DOES STUFF"),
                        Mockito.same(MapValue.EMPTY),
                        Mockito.eq(true),
                        Mockito.notNull());
        inOrder.verifyNoMoreInteractions();

        // make sure that the statement is present within the transaction as a result of run
        Assertions.assertThat(transaction.hasOpenStatement()).isTrue();
        Assertions.assertThat(transaction.latestStatementId()).isEqualTo(0);

        Assertions.assertThat(statement).isNotNull();

        Assertions.assertThat(transaction.getStatement(0)).isPresent().containsSame(statement);

        statement.close();

        // make sure that it is removed when explicitly closed
        Assertions.assertThat(transaction.hasOpenStatement()).isFalse();
        Assertions.assertThat(transaction.getStatement(0)).isNotPresent();
    }

    @Test
    void shouldCommitTransactions() throws TransactionFailureException, TransactionException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.isOpen()).isTrue();
        Assertions.assertThat(transaction.isValid()).isTrue();

        var bookmark = transaction.commit();

        Mockito.verify(this.boltTransaction).commit();
        Mockito.verify(this.boltTransaction).getBookmark();

        Assertions.assertThat(bookmark).isSameAs(this.bookmark);

        Assertions.assertThat(transaction.isOpen()).isFalse();
        Assertions.assertThat(transaction.isValid()).isTrue();
    }

    @Test
    void shouldRollbackTransactions() throws TransactionException, TransactionFailureException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.isOpen()).isTrue();
        Assertions.assertThat(transaction.isValid()).isTrue();

        transaction.rollback();

        Mockito.verify(this.boltTransaction).rollback();

        Assertions.assertThat(transaction.isOpen()).isFalse();
        Assertions.assertThat(transaction.isValid()).isTrue();
    }

    @Test
    void shouldInterruptTransactions() {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.isValid()).isTrue();

        transaction.interrupt();

        Mockito.verify(this.boltTransaction).markForTermination(Status.Transaction.Terminated);
        Mockito.verifyNoMoreInteractions(this.boltTransaction);

        Assertions.assertThat(transaction.isValid()).isTrue();
    }

    @Test
    void shouldValidateTransactions() {
        Mockito.doReturn(Optional.of(Status.Transaction.Terminated))
                .when(this.boltTransaction)
                .getReasonIfTerminated();

        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.validate()).isFalse();
    }

    @Test
    void shouldCloseTransactions()
            throws StatementException, TransactionCloseException, TransactionFailureException,
                    QueryExecutionKernelException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        Assertions.assertThat(transaction.isOpen()).isTrue();
        Assertions.assertThat(transaction.isValid()).isTrue();
        Assertions.assertThat(transaction.hasOpenStatement()).isFalse();

        var statement = transaction.run("SOME STATEMENT", MapValue.EMPTY);

        Mockito.verify(this.boltTransaction)
                .executeQuery(
                        Mockito.eq("SOME STATEMENT"),
                        Mockito.same(MapValue.EMPTY),
                        Mockito.eq(true),
                        Mockito.notNull());

        Assertions.assertThat(transaction.hasOpenStatement()).isTrue();
        Assertions.assertThat(statement.hasRemaining()).isTrue();

        transaction.close();

        var inOrder = Mockito.inOrder(this.boltTransaction);

        inOrder.verify(this.boltTransaction).rollback();
        inOrder.verify(this.boltTransaction).close();

        Assertions.assertThat(statement.hasRemaining()).isFalse();
        Assertions.assertThat(transaction.hasOpenStatement()).isFalse();
        Assertions.assertThat(transaction.isOpen()).isFalse();
        Assertions.assertThat(transaction.isValid()).isFalse();
    }

    @Test
    void shouldOmitRollbackOnCommittedTransaction() throws TransactionException, TransactionFailureException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        transaction.commit();
        transaction.close();

        Mockito.verify(this.boltTransaction, Mockito.never()).rollback();
    }

    @Test
    void shouldOmitRollbackOnRollbackTransaction() throws TransactionFailureException, TransactionException {
        var transaction = new TransactionImpl(
                "bolt-42", TransactionType.EXPLICIT, this.databaseReference, this.clock, this.boltTransaction);

        transaction.rollback();

        Mockito.verify(this.boltTransaction).rollback();

        transaction.close();

        Mockito.verify(this.boltTransaction, Mockito.times(1)).rollback();
    }
}
