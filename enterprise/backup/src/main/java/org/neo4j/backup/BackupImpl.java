/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.com.RequestContext.anonymous;

class BackupImpl implements TheBackupInterface
{
    private final StoreCopyServer storeCopyServer;
    private final ResponsePacker incrementalResponsePacker;
    private final LogicalTransactionStore logicalTransactionStore;
    private final Provider<StoreId> storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogFileInformation logFileInformation;

    public BackupImpl( StoreCopyServer storeCopyServer, LogicalTransactionStore logicalTransactionStore,
                       TransactionIdStore transactionIdStore, LogFileInformation logFileInformation,
                       Provider<StoreId> storeId )
    {
        this.storeCopyServer = storeCopyServer;
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.logFileInformation = logFileInformation;
        this.storeId = storeId;
        this.incrementalResponsePacker = new ResponsePacker( logicalTransactionStore, transactionIdStore, storeId );
    }

    public Response<Void> fullBackup( StoreWriter writer, boolean forensics )
    {
        try ( StoreWriter storeWriter = writer )
        {
            RequestContext copyStartContext = storeCopyServer.flushStoresAndStreamStoreFiles( storeWriter, forensics );
            long lastAppliedTxId = copyStartContext.lastAppliedTransaction();
            ResponsePacker responsePacker = new StoreCopyResponsePacker( logicalTransactionStore, transactionIdStore,
                    logFileInformation, storeId, lastAppliedTxId + 1, storeCopyServer.monitor() );
            return responsePacker.packTransactionStreamResponse( anonymous( lastAppliedTxId ), null/*no response object*/ );
        }
    }

    @Override
    public Response<Void> incrementalBackup( RequestContext context )
    {
        return incrementalResponsePacker.packTransactionStreamResponse( context, null );
    }
}
