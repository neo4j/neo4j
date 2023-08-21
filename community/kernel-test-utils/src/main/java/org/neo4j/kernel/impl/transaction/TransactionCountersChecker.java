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

import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.assertj.core.api.LongAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;

class TransactionCountersChecker {
    private final TransactionCounters counts;
    private final long started;
    private final long peakConcurrent;
    private final long activeRead;
    private final long activeWrite;
    private final long totalActive;
    private final long committedRead;
    private final long committedWrite;
    private final long totalCommitted;
    private final long rolledBackRead;
    private final long rolledBackWrite;
    private final long totalRolledBack;
    private final long terminatedRead;
    private final long terminatedWrite;
    private final long totalTerminated;

    static TransactionCountersChecker checkerFor(TransactionCounters counts) {
        try (var softly = new AutoCloseableSoftAssertions()) {
            return checkerFor(softly, counts);
        }
    }

    static TransactionCountersChecker checkerFor(SoftAssertions softly, TransactionCounters counts) {
        return new TransactionCountersChecker(softly, counts);
    }

    private TransactionCountersChecker(SoftAssertions softly, TransactionCounters counts) {
        this.counts = counts;
        // started
        started = verifyNonNegative(softly, "started", this.counts.getNumberOfStartedTransactions());

        // peak
        peakConcurrent =
                verifyNonNegative(softly, "peak concurrent", this.counts.getPeakConcurrentNumberOfTransactions());

        // Active
        activeRead = verifyNonNegative(softly, "active read", this.counts.getNumberOfActiveReadTransactions());
        activeWrite = verifyNonNegative(softly, "active write", this.counts.getNumberOfActiveWriteTransactions());
        totalActive = verifyNonNegative(softly, "total active", this.counts.getNumberOfActiveTransactions());
        verifyTotal(softly, "active", activeRead, activeWrite, totalActive);

        // Committed
        committedRead = verifyNonNegative(softly, "committed read", this.counts.getNumberOfCommittedReadTransactions());
        committedWrite =
                verifyNonNegative(softly, "committed write", this.counts.getNumberOfCommittedWriteTransactions());
        totalCommitted = verifyNonNegative(softly, "total committed", this.counts.getNumberOfCommittedTransactions());
        verifyTotal(softly, "committed", committedRead, committedWrite, totalCommitted);

        // Rolled Back
        rolledBackRead =
                verifyNonNegative(softly, "rolled back read", this.counts.getNumberOfRolledBackReadTransactions());
        rolledBackWrite =
                verifyNonNegative(softly, "rolled back write", this.counts.getNumberOfRolledBackWriteTransactions());
        totalRolledBack =
                verifyNonNegative(softly, "total rolled back", this.counts.getNumberOfRolledBackTransactions());
        verifyTotal(softly, "rolled back", rolledBackRead, rolledBackWrite, totalRolledBack);

        // Terminated
        terminatedRead =
                verifyNonNegative(softly, "terminated read", this.counts.getNumberOfTerminatedReadTransactions());
        terminatedWrite =
                verifyNonNegative(softly, "terminated write", this.counts.getNumberOfTerminatedWriteTransactions());
        totalTerminated =
                verifyNonNegative(softly, "total terminated", this.counts.getNumberOfTerminatedTransactions());
        verifyTotal(softly, "terminated", terminatedRead, terminatedWrite, totalTerminated);
    }

    public void verify(SoftAssertions softly, ExpectedDifference expectedDifference) {
        verifyStartedAndPeak(softly, expectedDifference);
        verifyActive(softly, expectedDifference);
        verifyCommitted(softly, expectedDifference);
        verifyRolledBack(softly, expectedDifference);
        verifyTerminated(softly, expectedDifference);
    }

    private void verifyStartedAndPeak(SoftAssertions softly, ExpectedDifference expectedDifference) {
        assertNonNegative(softly, "started", counts.getNumberOfStartedTransactions())
                .as(increasedBy("started", expectedDifference.started()))
                .isEqualTo(started + expectedDifference.started());

        assertNonNegative(softly, "peak concurrent", counts.getPeakConcurrentNumberOfTransactions())
                .as("number of peak concurrent transactions expected to be no less than it was")
                .isGreaterThanOrEqualTo(peakConcurrent)
                .as("number of peak concurrent transactions expected to be no less than the total number committed "
                        + "and rolled back transactions")
                .isGreaterThanOrEqualTo(expectedDifference.committed() + expectedDifference.rolledBack())
                .as("number of peak concurrent transactions expected to be no less than the total number committed "
                        + "and terminated transactions")
                .isGreaterThanOrEqualTo(expectedDifference.committed() + expectedDifference.terminated());
    }

    private void verifyActive(SoftAssertions softly, ExpectedDifference expectedDifference) {
        final var assertActiveRead =
                assertNonNegative(softly, "active read", counts.getNumberOfActiveReadTransactions());
        final var assertActiveWrite =
                assertNonNegative(softly, "active write", counts.getNumberOfActiveWriteTransactions());
        final var assertTotalActive = assertNonNegative(softly, "total active", counts.getNumberOfActiveTransactions());

        assertTotalActive
                .as(increasedBy("total active", expectedDifference.active()))
                .isEqualTo(totalActive + expectedDifference.active());
        if (expectedDifference.isWriteTx()) {
            assertActiveRead.as(unchanged("active read")).isEqualTo(activeRead);
            assertActiveWrite
                    .as(increasedBy("active write", expectedDifference.active()))
                    .isEqualTo(activeWrite + expectedDifference.active());
        } else {
            assertActiveRead
                    .as(increasedBy("active read", expectedDifference.active()))
                    .isEqualTo(activeRead + expectedDifference.active());
            assertActiveWrite.as(unchanged("active write")).isEqualTo(activeWrite);
        }
    }

    private void verifyCommitted(SoftAssertions softly, ExpectedDifference expectedDifference) {
        final var assertCommittedRead =
                assertNonNegative(softly, "committed read", counts.getNumberOfCommittedReadTransactions());
        final var assertCommittedWrite =
                assertNonNegative(softly, "committed write", counts.getNumberOfCommittedWriteTransactions());
        final var assertTotalCommitted =
                assertNonNegative(softly, "total committed", counts.getNumberOfCommittedTransactions());

        assertTotalCommitted
                .as(increasedBy("total committed", expectedDifference.committed()))
                .isEqualTo(totalCommitted + expectedDifference.committed());
        if (expectedDifference.isWriteTx()) {
            assertCommittedRead.as(unchanged("committed read")).isEqualTo(committedRead);
            assertCommittedWrite
                    .as(increasedBy("committed write", expectedDifference.committed()))
                    .isEqualTo(committedWrite + expectedDifference.committed());
        } else {
            assertCommittedRead
                    .as(increasedBy("committed read", expectedDifference.committed()))
                    .isEqualTo(committedRead + expectedDifference.committed());
            assertCommittedWrite.as(unchanged("committed write")).isEqualTo(committedWrite);
        }
    }

    private void verifyRolledBack(SoftAssertions softly, ExpectedDifference expectedDifference) {
        final var assertRolledBackRead =
                assertNonNegative(softly, "rolled back read", counts.getNumberOfRolledBackReadTransactions());
        final var assertRolledBackWrite =
                assertNonNegative(softly, "rolled back write", counts.getNumberOfRolledBackWriteTransactions());
        final var assertTotalRolledBack =
                assertNonNegative(softly, "total rolled back", counts.getNumberOfRolledBackTransactions());

        assertTotalRolledBack
                .as(increasedBy("total rolled back", expectedDifference.rolledBack()))
                .isEqualTo(totalRolledBack + expectedDifference.rolledBack());
        if (expectedDifference.isWriteTx()) {
            assertRolledBackRead.as(unchanged("rolled back read")).isEqualTo(rolledBackRead);
            assertRolledBackWrite
                    .as(increasedBy("rolled back write", expectedDifference.rolledBack()))
                    .isEqualTo(rolledBackWrite + expectedDifference.rolledBack());
        } else {
            assertRolledBackRead
                    .as(increasedBy("rolled back read", expectedDifference.rolledBack()))
                    .isEqualTo(rolledBackRead + expectedDifference.rolledBack());
            assertRolledBackWrite.as(unchanged("rolled back read")).isEqualTo(rolledBackWrite);
        }
    }

    private void verifyTerminated(SoftAssertions softly, ExpectedDifference expectedDifference) {
        final var assertTerminatedRead =
                assertNonNegative(softly, "terminated read", counts.getNumberOfTerminatedReadTransactions());
        final var assertTerminatedWrite =
                assertNonNegative(softly, "terminated write", counts.getNumberOfTerminatedWriteTransactions());
        final var assertTotalTerminated =
                assertNonNegative(softly, "total terminated", counts.getNumberOfTerminatedTransactions());

        assertTotalTerminated
                .as(increasedBy("total terminated", expectedDifference.terminated()))
                .isEqualTo(totalTerminated + expectedDifference.terminated());
        if (expectedDifference.isWriteTx()) {
            assertTerminatedRead.as(unchanged("terminated read")).isEqualTo(terminatedRead);
            assertTerminatedWrite
                    .as(increasedBy("terminated write", expectedDifference.terminated()))
                    .isEqualTo(terminatedWrite + expectedDifference.terminated());
        } else {
            assertTerminatedRead
                    .as(increasedBy("terminated read", expectedDifference.terminated()))
                    .isEqualTo(terminatedRead + expectedDifference.terminated());
            assertTerminatedWrite.as(unchanged("terminated write")).isEqualTo(terminatedWrite);
        }
    }

    private static long verifyNonNegative(SoftAssertions softly, String name, long value) {
        assertNonNegative(softly, name, value);
        return value;
    }

    private static LongAssert assertNonNegative(SoftAssertions softly, String name, long value) {
        return softly.assertThat(value).as(nonNegative(name)).isGreaterThanOrEqualTo(0);
    }

    private static void verifyTotal(SoftAssertions softly, String name, long read, long write, long total) {
        softly.assertThat(read + write)
                .as("total of read and write %s transactions should be the total number of %s transactions", name, name)
                .isEqualTo(total);
    }

    private static Description nonNegative(String name) {
        return new TextDescription("number of %s transactions is expected to be non-negative", name);
    }

    private static Description unchanged(String name) {
        return new TextDescription("number of %s transactions expected to not change", name);
    }

    private static Description increasedBy(String name, long value) {
        assertThat(value).as("given value expected to be non-negative").isGreaterThanOrEqualTo(0);
        if (value == 0) {
            return unchanged(name);
        }
        return new TextDescription("number of %s transactions expected to increase by %d", name, value);
    }

    record ExpectedDifference(
            boolean isWriteTx, int started, int active, int committed, int rolledBack, int terminated) {
        static final ExpectedDifference NONE = new ExpectedDifference(false, 0, 0, 0, 0, 0);

        ExpectedDifference isWriteTx(boolean isWriteTx) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference withStarted(int started) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference withActive(int active) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference withCommitted(int committed) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference withRolledBack(int rolledBack) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference withTerminated(int terminated) {
            return new ExpectedDifference(isWriteTx, started, active, committed, rolledBack, terminated);
        }

        ExpectedDifference verifyWith(TransactionCountersChecker checker) {
            try (var softly = new AutoCloseableSoftAssertions()) {
                return verifyWith(softly, checker);
            }
        }

        ExpectedDifference verifyWith(SoftAssertions softly, TransactionCountersChecker checker) {
            final var type = isWriteTx ? "write" : "read";
            verifyNonNegative(softly, "started " + type, started);
            verifyNonNegative(softly, "active " + type, active);
            verifyNonNegative(softly, "committed " + type, committed);
            verifyNonNegative(softly, "rolledBack " + type, rolledBack);
            verifyNonNegative(softly, "terminated " + type, terminated);
            checker.verify(softly, this);
            return this;
        }
    }
}
