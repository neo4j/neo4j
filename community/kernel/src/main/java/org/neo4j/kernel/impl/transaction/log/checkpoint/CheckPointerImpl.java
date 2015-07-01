/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer.PrintFormat.prefix;

public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer
{
    private final TransactionAppender appender;
    private final TransactionIdStore transactionIdStore;
    private final CheckPointThreshold threshold;
    private final StoreFlusher storeFlusher;
    private final LogPruning logPruning;
    private final KernelHealth kernelHealth;
    private final Log msgLog;
    private final CheckPointTracer tracer;

    public CheckPointerImpl( TransactionIdStore transactionIdStore, CheckPointThreshold threshold,
            StoreFlusher storeFlusher, LogPruning logPruning, TransactionAppender appender, KernelHealth kernelHealth,
            LogProvider logProvider, CheckPointTracer tracer )
    {
        this.appender = appender;
        this.transactionIdStore = transactionIdStore;
        this.threshold = threshold;
        this.storeFlusher = storeFlusher;
        this.logPruning = logPruning;
        this.kernelHealth = kernelHealth;
        this.msgLog = logProvider.getLog( CheckPointerImpl.class );
        this.tracer = tracer;
    }

    @Override
    public void start() throws Throwable
    {
        threshold.initialize( transactionIdStore.getLastClosedTransactionId() );
    }

    @Override
    public void forceCheckPoint() throws IOException
    {
        doCheckPoint( LogCheckPointEvent.NULL );
    }

    @Override
    public void checkPointIfNeeded() throws IOException
    {
        if ( threshold.isCheckPointingNeeded( transactionIdStore.getLastClosedTransactionId() ) )
        {
            try ( LogCheckPointEvent event = tracer.beginCheckPoint() )
            {
                doCheckPoint( event );
            }
        }
    }

    private void doCheckPoint( LogCheckPointEvent logCheckPointEvent ) throws IOException
    {
        long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
        long lastClosedTransactionId = lastClosedTransaction[0];
        LogPosition logPosition = new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
        msgLog.info( prefix( lastClosedTransactionId ) + "Starting check pointing..." );

        /*
         * Check kernel health before going into waiting for transactions to be closed, to avoid
         * getting into a scenario where we would await a condition that would potentially never
         * happen.
         */
        kernelHealth.assertHealthy( IOException.class );

        /*
         * First we flush the store. If we fail now or during the flush, on recovery we'll find the
         * earlier check point and replay from there all the log entries. Everything will be ok.
         */
        msgLog.info( prefix( lastClosedTransactionId ) + " Starting store flush..." );
        storeFlusher.forceEverything();

        /*
         * Check kernel health before going to write the next check point.  In case of a panic this check point
         * will be aborted, which is the safest alternative so that the next recovery will have a chance to
         * repair the damages.
         */
        kernelHealth.assertHealthy( IOException.class );
        appender.checkPoint( logPosition, logCheckPointEvent );
        threshold.checkPointHappened( lastClosedTransactionId );
        msgLog.info( prefix( lastClosedTransactionId ) + "Check pointing completed" );

        /*
         * Prune up to the version pointed from the latest check point,
         * since it might be an earlier version than the current log version.
         */
        logPruning.pruneLogs( logPosition.getLogVersion() );
    }
}
