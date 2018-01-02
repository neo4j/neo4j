/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.com.RequestContext.anonymous;

class BackupImpl implements TheBackupInterface
{
    static final String FULL_BACKUP_CHECKPOINT_TRIGGER = "full backup";

    private final StoreCopyServer storeCopyServer;
    private final ResponsePacker incrementalResponsePacker;
    private final LogicalTransactionStore logicalTransactionStore;
    private final Supplier<StoreId> storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogFileInformation logFileInformation;
    private final Logger logger;

    public BackupImpl( StoreCopyServer storeCopyServer, Monitors monitors,
            LogicalTransactionStore logicalTransactionStore, TransactionIdStore transactionIdStore,
            LogFileInformation logFileInformation, Supplier<StoreId> storeId, LogProvider logProvider )
    {
        this.storeCopyServer = storeCopyServer;
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.logFileInformation = logFileInformation;
        this.storeId = storeId;
        this.logger = logProvider.getLog( getClass() ).infoLogger();
        this.incrementalResponsePacker = new ResponsePacker( logicalTransactionStore, transactionIdStore, storeId );
    }

    @Override
    public Response<Void> fullBackup( StoreWriter writer, boolean forensics )
    {
        try ( StoreWriter storeWriter = writer )
        {
            logger.log( "Full backup started..." );
            RequestContext copyStartContext = storeCopyServer.flushStoresAndStreamStoreFiles(
                    FULL_BACKUP_CHECKPOINT_TRIGGER, storeWriter, forensics );
            ResponsePacker responsePacker = new StoreCopyResponsePacker( logicalTransactionStore,
                    transactionIdStore, logFileInformation, storeId,
                    copyStartContext.lastAppliedTransaction() + 1, storeCopyServer.monitor() ); // mandatory transaction id
            long optionalTransactionId = copyStartContext.lastAppliedTransaction();
            return responsePacker.packTransactionStreamResponse( anonymous( optionalTransactionId ), null/*no response object*/ );
        }
        finally
        {
            logger.log( "Full backup finished." );
        }
    }

    @Override
    public Response<Void> incrementalBackup( RequestContext context )
    {
        try
        {
            logger.log("Incremental backup started...");
            return incrementalResponsePacker.packTransactionStreamResponse( context, null );
        } finally {
            logger.log("Incremental backup finished.");
        }
    }
}
