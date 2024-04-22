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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.dedicated_transaction_appender;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionAppenderFactory {
    public static TransactionAppender createTransactionAppender(
            LogFiles logFiles,
            TransactionIdStore transactionIdStore,
            AppendIndexProvider appendIndexProvider,
            Config config,
            Panic databasePanic,
            JobScheduler scheduler,
            InternalLogProvider logProvider,
            TransactionMetadataCache metadataCache) {
        if (config.get(dedicated_transaction_appender)
                || "multiversion".equals(config.get(GraphDatabaseSettings.db_format))) {
            var queue = new TransactionLogQueue(
                    logFiles,
                    transactionIdStore,
                    databasePanic,
                    appendIndexProvider,
                    metadataCache,
                    scheduler,
                    logProvider);
            return new QueueTransactionAppender(queue);
        }

        return new BatchingTransactionAppender(
                logFiles, transactionIdStore, databasePanic, appendIndexProvider, metadataCache);
    }
}
