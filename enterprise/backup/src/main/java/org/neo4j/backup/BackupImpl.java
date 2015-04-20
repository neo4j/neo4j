/*
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
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.com.RequestContext.anonymous;

class BackupImpl implements TheBackupInterface
{
    private final StoreCopyServer storeCopyServer;
    private final ResponsePacker incrementalResponsePacker;
    private final LogicalTransactionStore logicalTransactionStore;
    private final Provider<StoreId> storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogFileInformation logFileInformation;

    public BackupImpl( StoreCopyServer storeCopyServer, Monitors monitors,
                       LogicalTransactionStore logicalTransactionStore, TransactionIdStore transactionIdStore,
                       LogFileInformation logFileInformation, Provider<StoreId> storeId )
    {
        this.storeCopyServer = storeCopyServer;
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.logFileInformation = logFileInformation;
        this.storeId = storeId;
        this.incrementalResponsePacker = new ResponsePacker( logicalTransactionStore, transactionIdStore, storeId );
    }

    @Override
    public Response<Void> fullBackup( StoreWriter writer, boolean forensics )
    {
        try ( StoreWriter storeWriter = writer )
        {
            RequestContext copyStartContext = storeCopyServer.flushStoresAndStreamStoreFiles( storeWriter, forensics );
            ResponsePacker responsePacker = new StoreCopyResponsePacker( logicalTransactionStore,
                    transactionIdStore, logFileInformation, storeId,
                    copyStartContext.lastAppliedTransaction() + 1, storeCopyServer.monitor() ); // mandatory transaction id
            long optionalTransactionId = copyStartContext.lastAppliedTransaction();
            return responsePacker.packTransactionStreamResponse( anonymous( optionalTransactionId ), null/*no response object*/ );
        }
    }

    @Override
    public Response<Void> incrementalBackup( RequestContext context )
    {
        return incrementalResponsePacker.packTransactionStreamResponse( context, null );
    }
}
