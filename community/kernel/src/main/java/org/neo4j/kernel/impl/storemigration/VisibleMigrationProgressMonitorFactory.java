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

import static java.lang.String.format;
import static org.neo4j.internal.helpers.Format.duration;

import java.time.Clock;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.LoggerPrintWriterAdaptor;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.util.VisibleForTesting;

public class VisibleMigrationProgressMonitorFactory {
    static MigrationProgressMonitor forMigration(InternalLog log) {
        return forMigration(log, Clock.systemUTC());
    }

    @VisibleForTesting
    static MigrationProgressMonitor forMigration(InternalLog log, Clock clock) {
        return new MigrationProgressMonitorImpl(
                log,
                clock,
                "Migrating",
                "Starting migration of database",
                "Successfully finished migration of database",
                "Starting transaction logs migration.",
                "Transaction logs migration completed.");
    }

    static MigrationProgressMonitor forUpgrade(InternalLog log) {
        return forUpgrade(log, Clock.systemUTC());
    }

    @VisibleForTesting
    static MigrationProgressMonitor forUpgrade(InternalLog log, Clock clock) {
        return new MigrationProgressMonitorImpl(
                log,
                clock,
                "Upgrading",
                "Starting upgrade of database",
                "Successfully finished upgrade of database",
                "Starting transaction logs upgrade.",
                "Transaction logs upgrade completed.");
    }

    public static MigrationProgressMonitor forSystemUpgrade(InternalLog log) {
        var clock = Clock.systemUTC();
        return new MigrationProgressMonitorImpl(
                log,
                clock,
                "Upgrading",
                "Starting upgrade of system database",
                "Successfully finished upgrade of system database",
                "Starting transaction logs upgrade.",
                "Transaction logs upgrade completed.");
    }

    private static class MigrationProgressMonitorImpl implements MigrationProgressMonitor {
        private final InternalLog log;
        private final Clock clock;
        private final String operation;
        private final String messageStarted;
        private final String txLogsUpgradeStarted;
        private final String txLogsUpgradeCompleted;
        private final String messageCompletedWithDuration;

        private int numStages;
        private int currentStage;
        private long startTime;

        MigrationProgressMonitorImpl(
                InternalLog log,
                Clock clock,
                String operation,
                String messageStarted,
                String messageCompleted,
                String txLogsUpgradeStarted,
                String txLogsUpgradeCompleted) {
            this.log = log;
            this.clock = clock;
            this.operation = operation;
            this.messageStarted = messageStarted;
            this.txLogsUpgradeStarted = txLogsUpgradeStarted;
            this.txLogsUpgradeCompleted = txLogsUpgradeCompleted;
            this.messageCompletedWithDuration = messageCompleted + ", took %s";
        }

        @Override
        public void started(int numStages) {
            this.numStages = numStages;
            log.info(messageStarted);
            startTime = clock.millis();
        }

        @Override
        public ProgressListener startSection(String name) {
            return startSection(name, 100);
        }

        public ProgressListener startSection(String name, int max) {
            log.info(format("%s %s (%d/%d):", operation, name, ++currentStage, numStages));
            var loggerPrintWriterAdaptor = new LoggerPrintWriterAdaptor(log, Level.INFO);
            var progressMonitorFactory = ProgressMonitorFactory.basicTextual(loggerPrintWriterAdaptor);
            return progressMonitorFactory.singlePart(name, max);
        }

        @Override
        public void completed() {
            long time = clock.millis() - startTime;
            log.info(messageCompletedWithDuration, duration(time));
        }

        @Override
        public void startTransactionLogsMigration() {
            log.info(txLogsUpgradeStarted);
        }

        @Override
        public void completeTransactionLogsMigration() {
            log.info(txLogsUpgradeCompleted);
        }
    }
}
