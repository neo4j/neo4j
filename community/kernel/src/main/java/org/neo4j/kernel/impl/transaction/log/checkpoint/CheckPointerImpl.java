/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.kernel.impl.store.counts.CountsSnapshot.NO_SNAPSHOT;


public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer
{
    private final TransactionAppender appender;
    private final TransactionIdStore transactionIdStore;
    private final CheckPointThreshold threshold;
    private final StorageEngine storageEngine;

    private final LogPruning logPruning;
    private final DatabaseHealth databaseHealth;
    private final Log msgLog;
    private final CheckPointTracer tracer;
    private final Lock lock;

    private long lastCheckPointedTx;
    private CountsSnapshot lastCountsSnapshot;

    public CheckPointerImpl( TransactionIdStore transactionIdStore, CheckPointThreshold threshold,
            StorageEngine storageEngine, LogPruning logPruning, TransactionAppender appender,
            DatabaseHealth databaseHealth, LogProvider logProvider, CheckPointTracer tracer )
    {
        this( transactionIdStore, threshold, storageEngine, logPruning, appender, databaseHealth, logProvider, tracer,
                new ReentrantLock() );
    }

    public CheckPointerImpl( TransactionIdStore transactionIdStore, CheckPointThreshold threshold,
            StorageEngine storageEngine, LogPruning logPruning, TransactionAppender appender,
            DatabaseHealth databaseHealth, LogProvider logProvider, CheckPointTracer tracer,
            Lock lock )
    {
        this.appender = appender;
        this.transactionIdStore = transactionIdStore;
        this.threshold = threshold;
        this.storageEngine = storageEngine;
        this.logPruning = logPruning;
        this.databaseHealth = databaseHealth;
        this.msgLog = logProvider.getLog( CheckPointerImpl.class );
        this.tracer = tracer;
        this.lock = lock;
    }

    @Override
    public void start() throws Throwable
    {
        threshold.initialize( transactionIdStore.getLastClosedTransactionId() );
    }

    @Override
    public CheckPointInfo forceCheckPoint( TriggerInfo info ) throws IOException
    {
        lock.lock();
        try
        {
            return doCheckPoint( info, LogCheckPointEvent.NULL );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public CheckPointInfo tryCheckPoint( TriggerInfo info ) throws IOException
    {
        if ( lock.tryLock() )
        {
            try
            {
                return doCheckPoint( info, LogCheckPointEvent.NULL );
            }
            finally
            {
                lock.unlock();
            }
        }
        else
        {
            lock.lock();
            try
            {
                msgLog.info(
                        info.describe( lastCheckPointedTx ) + " Check pointing was already running, completed now" );
                return new CheckPointInfo(lastCheckPointedTx, lastCountsSnapshot);
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    @Override
    public CheckPointInfo checkPointIfNeeded( TriggerInfo info ) throws IOException
    {
        lock.lock();
        try
        {
            if ( threshold.isCheckPointingNeeded( transactionIdStore.getLastClosedTransactionId(), info ) )
            {
                try ( LogCheckPointEvent event = tracer.beginCheckPoint() )
                {
                    return doCheckPoint( info, event );
                }
            }
            return new CheckPointInfo(-1, NO_SNAPSHOT);
        }
        finally
        {
            lock.unlock();
        }
    }

    private CheckPointInfo doCheckPoint( TriggerInfo triggerInfo, LogCheckPointEvent logCheckPointEvent ) throws IOException
    {
        long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
        long lastClosedTransactionId = lastClosedTransaction[0];
        LogPosition logPosition = new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );

        String prefix = triggerInfo.describe( lastClosedTransactionId );
        msgLog.info( prefix + " Starting check pointing..." );

        /*
         * Check kernel health before going into waiting for transactions to be closed, to avoid
         * getting into a scenario where we would await a condition that would potentially never
         * happen.
         */
        databaseHealth.assertHealthy( IOException.class );

        /*
         * First we flush the store. If we fail now or during the flush, on recovery we'll find the
         * earlier check point and replay from there all the log entries. Everything will be ok.
         */
        msgLog.info( prefix + " Starting store flush..." );
        storageEngine.flushAndForce();
        msgLog.info( prefix + " Store flush completed" );

        /*
         * Check kernel health before going to write the next check point.  In case of a panic this check point
         * will be aborted, which is the safest alternative so that the next recovery will have a chance to
         * repair the damages.
         */
        databaseHealth.assertHealthy( IOException.class );

        msgLog.info( prefix + " Starting Counts Store Snapshot Operation..." );
        CountsSnapshot snapshot = storageEngine.getSnapshotFromCountsStorageService( lastClosedTransactionId );
        msgLog.info( prefix + " Counts Store Snapshot completed. Booyah!" );

        msgLog.info( prefix + " Starting appending check point entry into the tx log..." );
        appender.checkPoint( logPosition, logCheckPointEvent, snapshot );
        threshold.checkPointHappened( lastClosedTransactionId );
        msgLog.info( prefix + " Appending check point entry into the tx log completed" );

        msgLog.info( prefix + " Check pointing completed" );

        /*
         * Prune up to the version pointed from the latest check point,
         * since it might be an earlier version than the current log version.
         */
        logPruning.pruneLogs( logPosition.getLogVersion() );

        lastCheckPointedTx = lastClosedTransactionId;
        lastCountsSnapshot = snapshot;
        return new CheckPointInfo(lastClosedTransactionId, snapshot);
    }

}
