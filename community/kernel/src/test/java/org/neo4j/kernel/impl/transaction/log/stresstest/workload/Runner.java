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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

public class Runner implements Callable<Long>
{
    private final File workingDirectory;
    private final BooleanSupplier condition;
    private final int threads;

    public Runner( File workingDirectory, BooleanSupplier condition, int threads )
    {
        this.workingDirectory = workingDirectory;
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
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 100_000 );
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

    private BatchingTransactionAppender createBatchingTransactionAppender( TransactionIdStore transactionIdStore,
            TransactionMetadataCache transactionMetadataCache, LogFiles logFiles )
    {
        Log log = NullLog.getInstance();
        KernelEventHandlers kernelEventHandlers = new KernelEventHandlers( log );
        DatabasePanicEventGenerator panicEventGenerator = new DatabasePanicEventGenerator( kernelEventHandlers );
        DatabaseHealth databaseHealth = new DatabaseHealth( panicEventGenerator, log );
        LogRotationImpl logRotation = new LogRotationImpl( NOOP_LOGROTATION_MONITOR, logFiles, databaseHealth );
        return new BatchingTransactionAppender( logFiles, logRotation,
                transactionMetadataCache, transactionIdStore, IdOrderingQueue.BYPASS, databaseHealth );
    }

    private LogFiles createLogFiles( TransactionIdStore transactionIdStore,
            FileSystemAbstraction fileSystemAbstraction ) throws IOException
    {
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        return LogFilesBuilder.builder( workingDirectory, fileSystemAbstraction )
                                                      .withTransactionIdStore(transactionIdStore)
                                                      .withLogVersionRepository( logVersionRepository )
                                                      .build();
    }

    private static final LogRotation.Monitor NOOP_LOGROTATION_MONITOR = new LogRotation.Monitor()
    {
        @Override
        public void startedRotating( long currentVersion )
        {

        }

        @Override
        public void finishedRotating( long currentVersion )
        {

        }
    };

}
