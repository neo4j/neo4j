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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.Recovery;
import org.neo4j.kernel.impl.transaction.state.RecoveryVisitor;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.String.format;

public class LoggingLogFileMonitor implements PhysicalLogFile.Monitor, RecoveryVisitor.Monitor, LogRotation.Monitor,
        Recovery.Monitor
{
    private int numberOfRecoveredTransactions;
    private long firstTransactionRecovered, lastTransactionRecovered;
    private final StringLogger logger;

    public LoggingLogFileMonitor( StringLogger logger )
    {
        this.logger = logger;
    }

    @Override
    public void recoveryRequired( long recoveredLogVersion )
    {
        logger.info( "Recovery required for log with version " + recoveredLogVersion );
    }

    @Override
    public void logRecovered()
    {
    }

    @Override
    public void recoveryCompleted()
    {
        if ( numberOfRecoveredTransactions != 0 )
        {
            logger.info( format( "Recovery completed. %d transactions, first:%d, last:%d recovered",
                    numberOfRecoveredTransactions, firstTransactionRecovered, lastTransactionRecovered ) );
        }
        else
        {
            logger.info( "No recovery required" );
        }
    }

    @Override
    public void startedRotating( long currentVersion )
    {
        logger.info( format( "Rotating log version:%d", currentVersion ) );
    }

    @Override
    public void finishedRotating( long currentVersion )
    {
        logger.info( format( "Finished rotating log version:%d", currentVersion ) );
    }

    @Override
    public void transactionRecovered( long txId )
    {
        if ( numberOfRecoveredTransactions == 0 )
        {
            firstTransactionRecovered = txId;
        }
        lastTransactionRecovered = txId;
        numberOfRecoveredTransactions++;
    }

    @Override
    public void opened( File logFile, long logVersion, long lastTransactionId, boolean clean )
    {
        logger.info( format( "Opened logical log [%s] version=%d, lastTxId=%d (%s)",
                logFile, logVersion, lastTransactionId,  (clean ? "clean" : "recovered") ) );
    }

    @Override
    public void failureToTruncate( File logFile, IOException e )
    {
        logger.warn( format( "Failed to truncate %s at correct size", logFile ), e );
    }
}
