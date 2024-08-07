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
package org.neo4j.kernel.recovery;

import java.time.Instant;
import java.util.function.Predicate;
import org.neo4j.internal.helpers.Format;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;

public interface RecoveryPredicate extends Predicate<CommittedCommandBatchRepresentation> {
    RecoveryPredicate ALL = new AllTransactionsPredicate();

    static RecoveryPredicate untilTransactionId(long txId) {
        return new TransactionIdPredicate(txId);
    }

    static RecoveryPredicate untilInstant(Instant date) {
        return new TransactionDatePredicate(date);
    }

    String describe();

    class AllTransactionsPredicate implements RecoveryPredicate {
        private AllTransactionsPredicate() {}

        @Override
        public String describe() {
            return "all transactions predicate.";
        }

        @Override
        public boolean test(CommittedCommandBatchRepresentation commandBatch) {
            return true;
        }
    }

    class TransactionIdPredicate implements RecoveryPredicate {
        private final long txId;

        private TransactionIdPredicate(long txId) {
            this.txId = txId;
        }

        @Override
        public boolean test(CommittedCommandBatchRepresentation commandBatch) {
            return commandBatch.txId() < txId;
        }

        @Override
        public String describe() {
            return "transaction id should be < " + txId;
        }
    }

    class TransactionDatePredicate implements RecoveryPredicate {
        private final Instant instant;

        private TransactionDatePredicate(Instant instant) {
            this.instant = instant;
        }

        @Override
        public boolean test(CommittedCommandBatchRepresentation commandBatch) {
            return commandBatch.timeWritten() < instant.toEpochMilli();
        }

        @Override
        public String describe() {
            return "transaction date should be before " + Format.date(instant);
        }
    }
}
