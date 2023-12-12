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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.time.FakeClock;

class VisibleMigrationProgressMonitorTest {
    @EnumSource(Operation.class)
    @ParameterizedTest
    void shouldReportAllPercentageSteps(Operation operation) {
        // GIVEN
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog log = logProvider.getLog(getClass());
        var monitor = operation.createMonitor(log, new FakeClock());
        monitor.started(1);

        // WHEN
        monitorSection(monitor, "First", 100, 40, 25, 23 /*these are too far*/, 10, 50);
        monitor.completed();

        // THEN
        verifySectionReportedCorrectly(operation, logProvider);
    }

    @EnumSource(Operation.class)
    @ParameterizedTest
    void progressNeverReportMoreThenHundredPercent(Operation operation) {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog log = logProvider.getLog(getClass());
        var monitor = operation.createMonitor(log, new FakeClock());

        monitor.started(1);
        monitorSection(monitor, "First", 100, 1, 10, 99, 170);
        monitor.completed();

        verifySectionReportedCorrectly(operation, logProvider);
    }

    @EnumSource(Operation.class)
    @ParameterizedTest
    void reportStartStopOfTransactionLogsMigration(Operation operation) {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog log = logProvider.getLog(getClass());
        var monitor = operation.createMonitor(log, new FakeClock());

        monitor.startTransactionLogsMigration();
        monitor.completeTransactionLogsMigration();

        assertThat(logProvider).containsMessages(operation.txLogsStartMessage(), operation.txLogsCompletedMessage());
    }

    @EnumSource(Operation.class)
    @ParameterizedTest
    void shouldIncludeDurationInCompletionMessage(Operation operation) {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        InternalLog log = logProvider.getLog(getClass());
        FakeClock clock = new FakeClock();
        var monitor = operation.createMonitor(log, clock);

        // when
        monitor.started(1);
        clock.forward(1500, TimeUnit.MILLISECONDS);
        monitor.completed();

        // then
        assertThat(logProvider).containsMessages("took 1s 500ms");
    }

    private static void verifySectionReportedCorrectly(Operation operation, AssertableLogProvider logProvider) {
        var messageMatcher = assertThat(logProvider);
        messageMatcher.containsMessages(operation.startMessage());
        for (int i = 10; i <= 100; i += 10) {
            messageMatcher.containsMessages(i + "%");
        }
        messageMatcher.containsMessages(operation.completedMessage());
        messageMatcher
                .forClass(VisibleMigrationProgressMonitorFactory.class)
                .forLevel(INFO)
                .doesNotContainMessage("110%");
    }

    private static void monitorSection(MigrationProgressMonitor monitor, String name, int max, int... steps) {
        try (ProgressListener progressListener = monitor.startSection(name, max)) {
            for (int step : steps) {
                progressListener.add(step);
            }
        }
    }

    private enum Operation {
        UPGRADE {
            @Override
            MigrationProgressMonitor createMonitor(InternalLog log, Clock clock) {
                return VisibleMigrationProgressMonitorFactory.forUpgrade(log, clock);
            }

            @Override
            String startMessage() {
                return "Starting upgrade of database";
            }

            @Override
            String completedMessage() {
                return "Successfully finished upgrade of database";
            }

            @Override
            String txLogsStartMessage() {
                return "Starting transaction logs upgrade.";
            }

            @Override
            String txLogsCompletedMessage() {
                return "Transaction logs upgrade completed.";
            }
        },
        MIGRATION {
            @Override
            MigrationProgressMonitor createMonitor(InternalLog log, Clock clock) {
                return VisibleMigrationProgressMonitorFactory.forMigration(log, clock);
            }

            @Override
            String startMessage() {
                return "Starting migration of database";
            }

            @Override
            String completedMessage() {
                return "Successfully finished migration of database";
            }

            @Override
            String txLogsStartMessage() {
                return "Starting transaction logs migration.";
            }

            @Override
            String txLogsCompletedMessage() {
                return "Transaction logs migration completed.";
            }
        };

        abstract MigrationProgressMonitor createMonitor(InternalLog log, Clock clock);

        abstract String startMessage();

        abstract String completedMessage();

        abstract String txLogsStartMessage();

        abstract String txLogsCompletedMessage();
    }
}
