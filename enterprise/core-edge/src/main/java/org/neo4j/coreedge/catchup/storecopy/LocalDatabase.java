/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class LocalDatabase implements Supplier<StoreId>, Lifecycle
{
    private final File storeDir;

    private final CopiedStoreRecovery copiedStoreRecovery;
    private final StoreFiles storeFiles;
    private final DataSourceManager dataSourceManager;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final Log log;

    private volatile StoreId storeId;
    private volatile DatabaseHealth databaseHealth;

    public LocalDatabase( File storeDir, CopiedStoreRecovery copiedStoreRecovery, StoreFiles storeFiles,
                          DataSourceManager dataSourceManager,
                          Supplier<TransactionIdStore> transactionIdStoreSupplier,
                          Supplier<DatabaseHealth> databaseHealthSupplier,
                          LogProvider logProvider )
    {
        this.storeDir = storeDir;
        this.copiedStoreRecovery = copiedStoreRecovery;
        this.storeFiles = storeFiles;
        this.dataSourceManager = dataSourceManager;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.log = logProvider.getLog( getClass() );
        this.storeId = StoreId.DEFAULT;
    }

    @Override
    public void init() throws Throwable
    {
        dataSourceManager.init();
    }

    @Override
    public void start() throws Throwable
    {
        dataSourceManager.start();
        org.neo4j.kernel.impl.store.StoreId kernelStoreId = dataSourceManager.getDataSource().getStoreId();
        storeId = new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(),
                kernelStoreId.getUpgradeTime(), kernelStoreId.getUpgradeId() );
        log.info( "My StoreId is: " + storeId );
    }

    @Override
    public void stop() throws Throwable
    {
        this.storeId = StoreId.DEFAULT;
        this.databaseHealth = null;
        dataSourceManager.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        dataSourceManager.shutdown();
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public void panic( Throwable cause )
    {
        getDatabaseHealth().panic( cause );
    }

    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> cause ) throws EXCEPTION
    {
        getDatabaseHealth().assertHealthy( cause );
    }

    private DatabaseHealth getDatabaseHealth()
    {
        if ( databaseHealth == null )
        {
            databaseHealth = databaseHealthSupplier.get();
        }
        return databaseHealth;
    }

    public void bringUpToDateOrReplaceStoreFrom( MemberId source, StoreId wantedStoreId, StoreFetcher storeFetcher ) throws StoreCopyFailedException
    {
        try
        {
            boolean successfullyCaughtUp = false;
            if ( wantedStoreId.equals( storeId ) )
            {
                successfullyCaughtUp = tryToCatchUp( source, storeFetcher );
            }

            if ( !successfullyCaughtUp )
            {
                storeFiles.delete( storeDir );
                copyWholeStoreFrom( source, wantedStoreId, storeFetcher );
            }
        }
        catch ( IOException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private boolean tryToCatchUp( MemberId source, StoreFetcher storeFetcher ) throws IOException, StoreCopyFailedException
    {
        return storeFetcher.tryCatchingUp( source, storeId, storeDir );
    }

    private void copyWholeStoreFrom( MemberId source, StoreId wantedStoreId, StoreFetcher storeFetcher ) throws IOException, StoreCopyFailedException
    {
        TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( storeDir );
        storeFetcher.copyStore( source, wantedStoreId, tempStore.storeDir() );
        copiedStoreRecovery.recoverCopiedStore( tempStore.storeDir() );
        storeFiles.moveTo( tempStore.storeDir(), storeDir );
    }

    public boolean isEmpty()
    {
        return transactionIdStoreSupplier.get().getLastCommittedTransactionId() == TransactionIdStore.BASE_TX_ID;
    }

    @Override
    public StoreId get()
    {
        return storeId();
    }

    public void ensureSameStoreId( MemberId memberId, StoreFetcher storeFetcher )
            throws StoreIdDownloadFailedException
    {
        StoreId localStoreId = storeId();
        StoreId remoteStoreId = storeFetcher.storeId( memberId );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException( format( "This edge machine cannot join the cluster. " +
                            "The local database is not empty and has a mismatching storeId: expected %s actual %s.",
                    remoteStoreId, localStoreId ) );
        }
    }
}
