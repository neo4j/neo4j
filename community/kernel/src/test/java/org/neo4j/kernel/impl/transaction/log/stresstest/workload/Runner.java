/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class Runner implements Callable<Long>
{
    private final DatabaseLayout databaseLayout;
    private final BooleanSupplier condition;
    private final int threads;

    public Runner( DatabaseLayout databaseLayout, BooleanSupplier condition, int threads )
    {
        this.databaseLayout = databaseLayout;
        this.condition = condition;
        this.threads = threads;
    }

    @Override
    public Long call() throws Exception
    {
        long lastCommittedTransactionId;

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Lifespan life = new Lifespan() )
        {
            TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();
            LogFiles logFiles = life.add( createLogFiles( transactionIdStore, fileSystem ) );

            TransactionAppender transactionAppender = life.add(
                    createBatchingTransactionAppender( transactionIdStore, transactionMetadataCache, logFiles ) );

            ExecutorService executorService = Executors.newFixedThreadPool( threads );
            try
            {
                Future<?>[] handlers = new Future[threads];
                for ( int i = 0; i < threads; i++ )
                {
                    TransactionRepresentationFactory factory = new TransactionRepresentationFactory();
                    Worker task = new Worker( transactionAppender, factory, condition );
                    handlers[i] = executorService.submit( task );
                }

                // wait for all the workers to complete
                for ( Future<?> handle : handlers )
                {
                    handle.get();
                }
            }
            finally
            {
                executorService.shutdown();
            }

            lastCommittedTransactionId = transactionIdStore.getLastCommittedTransactionId();
        }

        return lastCommittedTransactionId;
    }

    private static BatchingTransactionAppender createBatchingTransactionAppender( TransactionIdStore transactionIdStore,
            TransactionMetadataCache transactionMetadataCache, LogFiles logFiles )
    {
        Log log = NullLog.getInstance();
        DatabaseEventListeners databaseEventListeners = new DatabaseEventListeners( log );
        DatabasePanicEventGenerator panicEventGenerator = new DatabasePanicEventGenerator( databaseEventListeners, DEFAULT_DATABASE_NAME );
        Health databaseHealth = new DatabaseHealth( panicEventGenerator, log );
        LogRotationImpl logRotation = new LogRotationImpl( logFiles, Clock.systemUTC(), databaseHealth, LogRotationMonitorAdapter.EMPTY );
        return new BatchingTransactionAppender( logFiles, logRotation,
                transactionMetadataCache, transactionIdStore, databaseHealth );
    }

    private LogFiles createLogFiles( TransactionIdStore transactionIdStore,
            FileSystemAbstraction fileSystemAbstraction ) throws IOException
    {
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        return LogFilesBuilder.builder( databaseLayout, fileSystemAbstraction )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( new VersionAwareLogEntryReader( new TestCommandReaderFactory() ) )
                .withStoreId( StoreId.UNKNOWN )
                .build();
    }
}
