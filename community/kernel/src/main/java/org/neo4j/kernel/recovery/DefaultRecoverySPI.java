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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.RecoveryLabelScanWriterProvider;
import org.neo4j.kernel.impl.api.RecoveryLegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.RecoveryIndexingUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.RECOVERY;

public class DefaultRecoverySPI implements Recovery.SPI
{
    private final RecoveryLabelScanWriterProvider labelScanWriters;
    private final RecoveryLegacyIndexApplierLookup legacyIndexApplierLookup;
    private final StoreFlusher storeFlusher;
    private final NeoStores neoStores;
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fs;
    private final LogVersionRepository logVersionRepository;
    private final PositionToRecoverFrom positionToRecoverFrom;
    private final RecoveryIndexingUpdatesValidator indexUpdatesValidator;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final Visitor<CommittedTransactionRepresentation,Exception> recoveryVisitor;

    public DefaultRecoverySPI( RecoveryLabelScanWriterProvider labelScanWriters,
            RecoveryLegacyIndexApplierLookup legacyIndexApplierLookup,
            StoreFlusher storeFlusher, NeoStores neoStores,
            PhysicalLogFiles logFiles, FileSystemAbstraction fs,
            LogVersionRepository logVersionRepository, LatestCheckPointFinder checkPointFinder,
            RecoveryIndexingUpdatesValidator indexUpdatesValidator,
            TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore,
            TransactionRepresentationStoreApplier storeApplier )
    {
        this.labelScanWriters = labelScanWriters;
        this.legacyIndexApplierLookup = legacyIndexApplierLookup;
        this.storeFlusher = storeFlusher;
        this.neoStores = neoStores;
        this.logFiles = logFiles;
        this.fs = fs;
        this.logVersionRepository = logVersionRepository;
        this.indexUpdatesValidator = indexUpdatesValidator;
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.positionToRecoverFrom = new PositionToRecoverFrom( checkPointFinder );
        this.recoveryVisitor = new RecoveryVisitor( storeApplier, indexUpdatesValidator );
    }

    @Override
    public void forceEverything()
    {
        try
        {
            labelScanWriters.close();
            legacyIndexApplierLookup.close();
            indexUpdatesValidator.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        storeFlusher.forceEverything();
    }

    @Override
    public LogPosition getPositionToRecoverFrom() throws IOException
    {
        return positionToRecoverFrom.apply( logVersionRepository.getCurrentLogVersion() );
    }

    @Override
    public Visitor<CommittedTransactionRepresentation,Exception> startRecovery()
    {
        // Calling this method means that recovery is required, tell storage engine about it
        // This method will be called before recovery actually starts and so will ensure that
        // each store is aware that recovery will be performed. At this point all the stores have
        // already started btw.
        // Go and read more at {@link CommonAbstractStore#deleteIdGenerator()}
        neoStores.deleteIdGenerators();
        return recoveryVisitor;
    }

    @Override
    public TransactionCursor getTransactions( LogPosition position ) throws IOException
    {
        return logicalTransactionStore.getTransactions( position );
    }

    @Override
    public void allTransactionsRecovered( CommittedTransactionRepresentation lastRecoveredTransaction,
            LogPosition positionAfterLastRecoveredTransaction ) throws Exception
    {
        transactionIdStore.setLastCommittedAndClosedTransactionId(
                lastRecoveredTransaction.getCommitEntry().getTxId(),
                LogEntryStart.checksum( lastRecoveredTransaction.getStartEntry() ),
                lastRecoveredTransaction.getCommitEntry().getTimeWritten(),
                positionAfterLastRecoveredTransaction.getByteOffset(),
                positionAfterLastRecoveredTransaction.getLogVersion() );

        fs.truncate( logFiles.getLogFileForVersion( positionAfterLastRecoveredTransaction.getLogVersion() ),
                positionAfterLastRecoveredTransaction.getByteOffset() );
    }

    static class RecoveryVisitor implements Visitor<CommittedTransactionRepresentation,Exception>
    {
        private final TransactionRepresentationStoreApplier storeApplier;
        private final RecoveryIndexingUpdatesValidator indexUpdatesValidator;

        public RecoveryVisitor( TransactionRepresentationStoreApplier storeApplier,
                RecoveryIndexingUpdatesValidator indexUpdatesValidator )
        {
            this.storeApplier = storeApplier;
            this.indexUpdatesValidator = indexUpdatesValidator;
        }

        @Override
        public boolean visit( CommittedTransactionRepresentation transaction ) throws Exception
        {
            TransactionRepresentation txRepresentation = transaction.getTransactionRepresentation();
            long txId = transaction.getCommitEntry().getTxId();
            try ( LockGroup locks = new LockGroup();
                    ValidatedIndexUpdates indexUpdates = prepareIndexUpdates( txRepresentation ) )
            {
                storeApplier.apply( txRepresentation, indexUpdates, locks, txId, RECOVERY );
            }
            return false;
        }

        /**
         * Recovery operates under a condition that all index updates are valid because otherwise they have no chance to
         * appear in write ahead log.
         * This step is still needed though, because it is not only about validation of index sizes but also about
         * inferring {@link org.neo4j.kernel.api.index.NodePropertyUpdate}s from commands in transaction state and
         * grouping those by {@link org.neo4j.kernel.api.index.IndexUpdater}s.
         */
        private ValidatedIndexUpdates prepareIndexUpdates( TransactionRepresentation txRepresentation ) throws IOException
        {
            return indexUpdatesValidator.validate( txRepresentation );
        }
    }
}
