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
package org.neo4j.kernel.impl.storemigration;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.Format.duration;

import java.time.Clock;
import org.neo4j.common.ProgressReporter;
import org.neo4j.kernel.impl.util.monitoring.LogProgressReporter;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.util.VisibleForTesting;

class VisibleMigrationProgressMonitorFactory {
    static MigrationProgressMonitor forMigration(InternalLog log) {
        return forMigration(log, Clock.systemUTC());
    }

    @VisibleForTesting
    static MigrationProgressMonitor forMigration(InternalLog log, Clock clock) {
        return new MigrationProgressMonitor() {
            static final String MESSAGE_STARTED = "Starting migration of database";
            static final String MESSAGE_COMPLETED = "Successfully finished migration of database";
            static final String TX_LOGS_MIGRATION_STARTED = "Starting transaction logs migration.";
            static final String TX_LOGS_MIGRATION_COMPLETED = "Transaction logs migration completed.";
            private static final String MESSAGE_COMPLETED_WITH_DURATION = MESSAGE_COMPLETED + ", took %s";

            private int numStages;
            private int currentStage;
            private long startTime;

            @Override
            public void started(int numStages) {
                this.numStages = numStages;
                log.info(MESSAGE_STARTED);
                startTime = clock.millis();
            }

            @Override
            public ProgressReporter startSection(String name) {
                log.info(format("Migrating %s (%d/%d):", name, ++currentStage, numStages));
                return new LogProgressReporter(log);
            }

            @Override
            public void completed() {
                long time = clock.millis() - startTime;
                log.info(MESSAGE_COMPLETED_WITH_DURATION, duration(time));
            }

            @Override
            public void startTransactionLogsMigration() {
                log.info(TX_LOGS_MIGRATION_STARTED);
            }

            @Override
            public void completeTransactionLogsMigration() {
                log.info(TX_LOGS_MIGRATION_COMPLETED);
            }
        };
    }

    static MigrationProgressMonitor forUpgrade(InternalLog log) {
        return forUpgrade(log, Clock.systemUTC());
    }

    @VisibleForTesting
    static MigrationProgressMonitor forUpgrade(InternalLog log, Clock clock) {
        return new MigrationProgressMonitor() {
            static final String MESSAGE_STARTED = "Starting upgrade of database";
            static final String MESSAGE_COMPLETED = "Successfully finished upgrade of database";
            static final String TX_LOGS_UPGRADE_STARTED = "Starting transaction logs upgrade.";
            static final String TX_LOGS_UPGRADE_COMPLETED = "Transaction logs upgrade completed.";
            private static final String MESSAGE_COMPLETED_WITH_DURATION = MESSAGE_COMPLETED + ", took %s";

            private int numStages;
            private int currentStage;
            private long startTime;

            @Override
            public void started(int numStages) {
                this.numStages = numStages;
                log.info(MESSAGE_STARTED);
                startTime = clock.millis();
            }

            @Override
            public ProgressReporter startSection(String name) {
                log.info(format("Upgrading %s (%d/%d):", name, ++currentStage, numStages));
                return new LogProgressReporter(log);
            }

            @Override
            public void completed() {
                long time = clock.millis() - startTime;
                log.info(MESSAGE_COMPLETED_WITH_DURATION, duration(time));
            }

            @Override
            public void startTransactionLogsMigration() {
                log.info(TX_LOGS_UPGRADE_STARTED);
            }

            @Override
            public void completeTransactionLogsMigration() {
                log.info(TX_LOGS_UPGRADE_COMPLETED);
            }
        };
    }
}
