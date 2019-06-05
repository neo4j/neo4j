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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class DefaultRecoveryService implements RecoveryService
{
    private final RecoveryStartInformationProvider recoveryStartInformationProvider;
    private final StorageEngine storageEngine;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final LogVersionRepository logVersionRepository;
    private final LogFiles logFiles;
    private final StoreInfo storeInfo;
    private final Log log;

    DefaultRecoveryService( StorageEngine storageEngine, LogTailScanner logTailScanner, TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore, LogVersionRepository logVersionRepository, LogFiles logFiles, StoreInfo storeInfo,
            RecoveryStartInformationProvider.Monitor monitor, Log log )
    {
        this.storageEngine = storageEngine;
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.logVersionRepository = logVersionRepository;
        this.logFiles = logFiles;
        this.storeInfo = storeInfo;
        this.log = log;
        this.recoveryStartInformationProvider = new RecoveryStartInformationProvider( logTailScanner, monitor );
    }

    @Override
    public RecoveryStartInformation getRecoveryStartInformation() throws IOException
    {
        RecoveryStartInformation recoveryStartInformation = recoveryStartInformationProvider.get();
        if ( storeInfo.isAllStoreFilesPresent() )
        {
            // regular recovery
            return recoveryStartInformation;
        }
        // not all store files are present and forced or recovery from the beginning is required
        if ( recoveryStartInformation.isMissingLogs() )
        {
            // nothing we can help with: tx logs missing and files are missing so return information about missing log files
            return recoveryStartInformation;
        }
        // in all other scenarios we try to recover as much as possible from the beginning of available transaction logs
        long lowestAvailableLogFile = logFiles.getLowestLogVersion();
        LogPosition firstAvailableLogFileStart = new LogPosition( lowestAvailableLogFile, LOG_HEADER_SIZE );
        long firstTxToRecoverId = logFiles.getLogFileInformation().getFirstExistingEntryId();
        return new RecoveryStartInformation( firstAvailableLogFileStart, firstTxToRecoverId );
    }

    @Override
    public void startRecovery()
    {
        // Calling this method means that recovery is required, tell storage engine about it
        // This method will be called before recovery actually starts and so will ensure that
        // each store is aware that recovery will be performed. At this point all the stores have
        // already started btw.
        // Go and read more at {@link CommonAbstractStore#deleteIdGenerator()}
        storageEngine.prepareForRecoveryRequired();
    }

    @Override
    public RecoveryApplier getRecoveryApplier( TransactionApplicationMode mode ) throws Exception
    {
        return new RecoveryVisitor( storageEngine, mode );
    }

    @Override
    public TransactionCursor getTransactions( LogPosition position ) throws IOException
    {
        return logicalTransactionStore.getTransactions( position );
    }

    @Override
    public TransactionCursor getTransactionsInReverseOrder( LogPosition position ) throws IOException
    {
        return logicalTransactionStore.getTransactionsInReverseOrder( position );
    }

    @Override
    public void transactionsRecovered( CommittedTransactionRepresentation lastRecoveredTransaction,
            LogPosition positionAfterLastRecoveredTransaction, boolean missingLogs )
    {
        if ( missingLogs )
        {
            // in case if logs are missing we need to reset position of last committed transaction since
            // this information influencing checkpoint that will be created and if we will not gonna do that
            // it will still reference old offset from logs that are gone and as result log position in checkpoint record will be incorrect
            // and that can cause partial next recovery.
            long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
            long logVersion = lastClosedTransaction[1];
            log.warn( "Recovery detected that transaction logs were missing. " +
                    "Resetting offset of last closed transaction to point to the head of %d transaction log file.", logVersion );
            transactionIdStore.resetLastClosedTransaction( lastClosedTransaction[0], logVersion, LogHeader.LOG_HEADER_SIZE );
            return;
        }
        long recoveredTransactionLogVersion = positionAfterLastRecoveredTransaction.getLogVersion();
        long recoveredTransactionOffset = positionAfterLastRecoveredTransaction.getByteOffset();
        if ( lastRecoveredTransaction != null )
        {
            LogEntryCommit commitEntry = lastRecoveredTransaction.getCommitEntry();
            transactionIdStore.setLastCommittedAndClosedTransactionId(
                    commitEntry.getTxId(),
                    LogEntryStart.checksum( lastRecoveredTransaction.getStartEntry() ),
                    commitEntry.getTimeWritten(),
                    recoveredTransactionOffset,
                    recoveredTransactionLogVersion );
        }
        logVersionRepository.setCurrentLogVersion( recoveredTransactionLogVersion );
    }

    static class RecoveryVisitor implements RecoveryApplier
    {
        private final StorageEngine storageEngine;
        private final TransactionApplicationMode mode;

        RecoveryVisitor( StorageEngine storageEngine, TransactionApplicationMode mode )
        {
            this.storageEngine = storageEngine;
            this.mode = mode;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation transaction ) throws Exception
        {
            TransactionRepresentation txRepresentation = transaction.getTransactionRepresentation();
            long txId = transaction.getCommitEntry().getTxId();
            TransactionToApply tx = new TransactionToApply( txRepresentation, txId );
            tx.commitment( NO_COMMITMENT, txId );
            tx.logPosition( transaction.getStartEntry().getStartPosition() );
            storageEngine.apply( tx, mode );
            return false;
        }

        @Override
        public void close()
        {   // nothing to close
        }
    }
}
