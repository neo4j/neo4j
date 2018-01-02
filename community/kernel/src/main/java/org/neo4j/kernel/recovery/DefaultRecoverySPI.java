/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailScanner;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.recovery.Recovery.RecoveryApplier;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;

public class DefaultRecoverySPI implements Recovery.SPI
{
    private final PositionToRecoverFrom positionToRecoverFrom;
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fs;
    private final StorageEngine storageEngine;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;

    public DefaultRecoverySPI(
            StorageEngine storageEngine,
            PhysicalLogFiles logFiles, FileSystemAbstraction fs,
            LogTailScanner logTailScanner,
            TransactionIdStore transactionIdStore, LogicalTransactionStore logicalTransactionStore,
            PositionToRecoverFrom.Monitor monitor )
    {
        this.storageEngine = storageEngine;
        this.logFiles = logFiles;
        this.fs = fs;
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.positionToRecoverFrom = new PositionToRecoverFrom( logTailScanner, monitor );
    }

    @Override
    public LogPosition getPositionToRecoverFrom() throws IOException
    {
        return positionToRecoverFrom.get();
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
    public void allTransactionsRecovered( CommittedTransactionRepresentation lastRecoveredTransaction,
            LogPosition positionAfterLastRecoveredTransaction ) throws Exception
    {
        if ( lastRecoveredTransaction != null )
        {
            transactionIdStore.setLastCommittedAndClosedTransactionId(
                    lastRecoveredTransaction.getCommitEntry().getTxId(),
                    LogEntryStart.checksum( lastRecoveredTransaction.getStartEntry() ),
                    lastRecoveredTransaction.getCommitEntry().getTimeWritten(),
                    positionAfterLastRecoveredTransaction.getByteOffset(),
                    positionAfterLastRecoveredTransaction.getLogVersion() );
        }

        fs.truncate( logFiles.getLogFileForVersion( positionAfterLastRecoveredTransaction.getLogVersion() ),
                positionAfterLastRecoveredTransaction.getByteOffset() );
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
        public void close() throws Exception
        {   // nothing to close
        }
    }
}
