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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.pagecache.IOLimiter;
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

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.duration;

public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer
{
    private final TransactionAppender appender;
    private final TransactionIdStore transactionIdStore;
    private final CheckPointThreshold threshold;
    private final StorageEngine storageEngine;
    private final LogPruning logPruning;
    private final DatabaseHealth databaseHealth;
    private final IOLimiter ioLimiter;
    private final Log msgLog;
    private final CheckPointTracer tracer;
    private final StoreCopyCheckPointMutex mutex;

    private long lastCheckPointedTx;

    public CheckPointerImpl(
            TransactionIdStore transactionIdStore,
            CheckPointThreshold threshold,
            StorageEngine storageEngine,
            LogPruning logPruning,
            TransactionAppender appender,
            DatabaseHealth databaseHealth,
            LogProvider logProvider,
            CheckPointTracer tracer,
            IOLimiter ioLimiter,
            StoreCopyCheckPointMutex mutex )
    {
        this.appender = appender;
        this.transactionIdStore = transactionIdStore;
        this.threshold = threshold;
        this.storageEngine = storageEngine;
        this.logPruning = logPruning;
        this.databaseHealth = databaseHealth;
        this.ioLimiter = ioLimiter;
        this.msgLog = logProvider.getLog( CheckPointerImpl.class );
        this.tracer = tracer;
        this.mutex = mutex;
    }

    @Override
    public void start()
    {
        threshold.initialize( transactionIdStore.getLastClosedTransactionId() );
    }

    @Override
    public long forceCheckPoint( TriggerInfo info ) throws IOException
    {
        ioLimiter.disableLimit();
        try ( Resource lock = mutex.checkPoint() )
        {
            return doCheckPoint( info, LogCheckPointEvent.NULL );
        }
        finally
        {
            ioLimiter.enableLimit();
        }
    }

    @Override
    public long tryCheckPoint( TriggerInfo info ) throws IOException
    {
        ioLimiter.disableLimit();
        try
        {
            Resource lockAttempt = mutex.tryCheckPoint();
            if ( lockAttempt != null )
            {
                try ( Resource lock = lockAttempt )
                {
                    return doCheckPoint( info, LogCheckPointEvent.NULL );
                }
            }
            else
            {
                try ( Resource lock = mutex.checkPoint() )
                {
                    msgLog.info( info.describe( lastCheckPointedTx ) +
                                 " Check pointing was already running, completed now" );
                    return lastCheckPointedTx;
                }
            }
        }
        finally
        {
            ioLimiter.enableLimit();
        }
    }

    @Override
    public long checkPointIfNeeded( TriggerInfo info ) throws IOException
    {
        if ( threshold.isCheckPointingNeeded( transactionIdStore.getLastClosedTransactionId(), info ) )
        {
            try ( LogCheckPointEvent event = tracer.beginCheckPoint();
                    Resource lock = mutex.checkPoint() )
            {
                return doCheckPoint( info, event );
            }
        }
        return -1;
    }

    private long doCheckPoint( TriggerInfo triggerInfo, LogCheckPointEvent logCheckPointEvent ) throws IOException
    {
        try
        {
            long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
            long lastClosedTransactionId = lastClosedTransaction[0];
            LogPosition logPosition = new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
            String prefix = triggerInfo.describe( lastClosedTransactionId );
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
            msgLog.info( prefix + " checkpoint started..." );
            long startTime = currentTimeMillis();
            storageEngine.flushAndForce( ioLimiter );
            /*
             * Check kernel health before going to write the next check point.  In case of a panic this check point
             * will be aborted, which is the safest alternative so that the next recovery will have a chance to
             * repair the damages.
             */
            databaseHealth.assertHealthy( IOException.class );
            appender.checkPoint( logPosition, logCheckPointEvent );
            threshold.checkPointHappened( lastClosedTransactionId );
            msgLog.info( prefix + " checkpoint completed in " + duration( currentTimeMillis() - startTime ) );
            /*
             * Prune up to the version pointed from the latest check point,
             * since it might be an earlier version than the current log version.
             */
            logPruning.pruneLogs( logPosition.getLogVersion() );
            lastCheckPointedTx = lastClosedTransactionId;
            return lastClosedTransactionId;
        }
        catch ( Throwable t )
        {
            // Why only log failure here? It's because check point can potentially be made from various
            // points of execution e.g. background thread triggering check point if needed and during
            // shutdown where it's better to have more control over failure handling.
            msgLog.error( "Checkpoint failed", t );
            throw t;
        }
    }

    @Override
    public long lastCheckPointedTransactionId()
    {
        return lastCheckPointedTx;
    }
}
