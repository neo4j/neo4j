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
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriterV1;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer.PrintFormat.prefix;

public class CheckPointerImpl extends LifecycleAdapter implements CheckPointer
{
    private final LogFile logFile;
    private final TransactionIdStore transactionIdStore;
    private final CheckPointThreshold enabler;
    private TransactionLogWriter transactionLogWriter;
    private final StoreFlusher storeFlusher;
    private final LogPruning logPruning;
    private final KernelHealth kernelHealth;
    private final Log msgLog;
    private WritableLogChannel channel;

    public CheckPointerImpl( LogFile logFile, TransactionIdStore transactionIdStore, CheckPointThreshold enabler,
            StoreFlusher storeFlusher, LogPruning logPruning, KernelHealth kernelHealth, LogProvider logProvider )
    {
        this.logFile = logFile;
        this.transactionIdStore = transactionIdStore;
        this.enabler = enabler;
        this.storeFlusher = storeFlusher;
        this.logPruning = logPruning;
        this.kernelHealth = kernelHealth;
        this.msgLog = logProvider.getLog( CheckPointerImpl.class );
    }

    @Override
    public void start() throws Throwable
    {
        this.channel = logFile.getWriter();
        this.transactionLogWriter =
                new TransactionLogWriter( new LogEntryWriterV1( channel, NeoCommandHandler.EMPTY ) );
    }

    @Override
    public void checkPointIfNeeded( LogAppendEvent logAppendEvent ) throws IOException
    {
        if ( enabler.isCheckPointingNeeded() )
        {
            synchronized ( logFile )
            {
                if ( enabler.isCheckPointingNeeded() )
                {
                    try ( LogCheckPointEvent event = logAppendEvent.beginCheckPoint() )
                    {
                        doCheckPoint();
                    }
                }
            }
        }
    }

    @Override
    public void forceCheckPoint() throws IOException
    {
        synchronized ( logFile )
        {
            doCheckPoint();
        }
    }

    private void doCheckPoint() throws IOException
    {
        long[] lastCommittedTransaction = transactionIdStore.getLastCommittedTransaction();
        long lastCommittedTransactionId = lastCommittedTransaction[0];
        LogPosition logPosition =
                new LogPosition( lastCommittedTransaction[2], lastCommittedTransaction[3] );
        msgLog.info( prefix( lastCommittedTransactionId ) + "Starting check pointing..." );

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
        msgLog.info( prefix( lastCommittedTransactionId ) + " Starting store flush..." );
        storeFlusher.forceEverything();

        synchronized ( channel ) // TODO: is this needed?
        {
            /*
             * Check kernel health before going to write the next check point.
             * In case of a panic this rotation will be aborted, which is the safest alternative
             * so that the next recovery will have a chance to repair the damages.
             */
            kernelHealth.assertHealthy( IOException.class );
            transactionLogWriter.checkPoint( logPosition );
            channel.emptyBufferIntoChannelAndClearIt();
            channel.force();
        }

        enabler.checkPointHappened();
        msgLog.info( prefix( lastCommittedTransactionId ) + "Check pointing completed" );

        /*
         * prune up to the version pointed from the latest check point,
         * since it might be an earlier version than the current log version
         */
        logPruning.pruneLogs( logPosition.getLogVersion() );
    }
}
