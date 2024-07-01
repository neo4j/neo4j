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

import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

public interface RecoveryMonitor {
    default void recoveryRequired(RecoveryStartInformation recoveryStartInfo) {}

    default void batchRecovered(CommittedCommandBatch committedBatch) {}

    default void recoveryCompleted(long recoveryTimeInMilliseconds, RecoveryMode mode) {}

    default void reverseStoreRecoveryCompleted(long lowestRecoveredAppendIndex) {}

    default void failToRecoverTransactionsAfterCommit(
            Throwable t, CommittedCommandBatch.BatchInformation commandBatch, LogPosition recoveryToPosition) {}

    default void failToRecoverTransactionsAfterPosition(Throwable t, LogPosition recoveryFromPosition) {}

    default void partialRecovery(
            RecoveryPredicate recoveryPredicate, CommittedCommandBatch.BatchInformation commandBatch) {}

    default void batchApplySkipped(CommittedCommandBatch committedBatch) {}

    default void rollbackTransaction(long transactionId, long appendIndex) {}
}
