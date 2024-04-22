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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;

import static org.neo4j.kernel.impl.transaction.log.TransactionAppenderFactory.createTransactionAppender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.util.concurrent.Futures;

public class Runner implements Callable<Long> {
    private final DatabaseLayout databaseLayout;
    private final BooleanSupplier condition;
    private final int threads;

    public Runner(DatabaseLayout databaseLayout, BooleanSupplier condition, int threads) {
        this.databaseLayout = databaseLayout;
        this.condition = condition;
        this.threads = threads;
    }

    @Override
    public Long call() throws Exception {
        long lastCommittedTransactionId;

        try (FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                var jobScheduler = new ThreadPoolJobScheduler();
                Lifespan life = new Lifespan()) {
            TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
            LogFiles logFiles = life.add(createLogFiles(transactionIdStore, fileSystem));

            TransactionAppender transactionAppender = life.add(createBatchingTransactionAppender(
                    logFiles, transactionIdStore, new SimpleAppendIndexProvider(), Config.defaults(), jobScheduler));

            ExecutorService executorService = Executors.newFixedThreadPool(threads);
            try {
                List<Future<?>> handlers = new ArrayList<>(threads);
                for (int i = 0; i < threads; i++) {
                    TransactionRepresentationFactory factory = new TransactionRepresentationFactory();
                    Worker task = new Worker(transactionAppender, factory, condition);
                    handlers.add(executorService.submit(task));
                }

                // wait for all the workers to complete
                Futures.getAll(handlers);
            } finally {
                executorService.shutdown();
            }

            lastCommittedTransactionId = transactionIdStore.getLastCommittedTransactionId();
        }

        return lastCommittedTransactionId;
    }

    private static TransactionAppender createBatchingTransactionAppender(
            LogFiles logFiles,
            TransactionIdStore transactionIdStore,
            AppendIndexProvider appendIndexProvider,
            Config config,
            JobScheduler jobScheduler) {
        InternalLog log = NullLog.getInstance();
        DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, log);
        return createTransactionAppender(
                logFiles,
                transactionIdStore,
                appendIndexProvider,
                config,
                databaseHealth,
                jobScheduler,
                NullLogProvider.getInstance(),
                new TransactionMetadataCache());
    }

    private LogFiles createLogFiles(TransactionIdStore transactionIdStore, FileSystemAbstraction fileSystemAbstraction)
            throws IOException {
        AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(
                        databaseLayout, fileSystemAbstraction, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withTransactionIdStore(transactionIdStore)
                .withLogVersionRepository(logVersionRepository)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }
}
