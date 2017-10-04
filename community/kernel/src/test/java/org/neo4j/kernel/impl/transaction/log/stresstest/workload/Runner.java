/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
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
            TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore();
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 100_000 );
            LogHeaderCache logHeaderCache = new LogHeaderCache( 1000 );

            LogFile logFile = life.add( createPhysicalLogFile( transactionIdStore, logHeaderCache, fileSystem ) );

            TransactionAppender transactionAppender = life.add(
                    createBatchingTransactionAppender( transactionIdStore, transactionMetadataCache, logFile ) );

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
            TransactionMetadataCache transactionMetadataCache, LogFile logFile )
    {
        Log log = NullLog.getInstance();
        KernelEventHandlers kernelEventHandlers = new KernelEventHandlers( log );
        DatabasePanicEventGenerator panicEventGenerator = new DatabasePanicEventGenerator( kernelEventHandlers );
        DatabaseHealth databaseHealth = new DatabaseHealth( panicEventGenerator, log );
        LogRotationImpl logRotation = new LogRotationImpl( NOOP_LOGROTATION_MONITOR, logFile, databaseHealth );
        return new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, IdOrderingQueue.BYPASS, databaseHealth );
    }

    private PhysicalLogFile createPhysicalLogFile( TransactionIdStore transactionIdStore, LogHeaderCache logHeaderCache,
            FileSystemAbstraction fileSystemAbstraction )
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( workingDirectory, fileSystemAbstraction );
        long rotateAtSize = Settings.BYTES.apply(
                GraphDatabaseSettings.logical_log_rotation_threshold.getDefaultValue() );
        DeadSimpleLogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 0 );
        return new PhysicalLogFile( fileSystemAbstraction, logFiles, rotateAtSize,
                transactionIdStore::getLastCommittedTransactionId, logVersionRepository, PhysicalLogFile.NO_MONITOR,
                logHeaderCache );
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
