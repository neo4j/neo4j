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

import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class LocalDatabase implements Supplier<StoreId>, Lifecycle
{
    private final File storeDir;

    private final StoreFiles storeFiles;
    private final DataSourceManager dataSourceManager;
    private final PageCache pageCache;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;

    private volatile StoreId storeId;
    private volatile DatabaseHealth databaseHealth;
    private boolean started = false;

    public LocalDatabase( File storeDir, StoreFiles storeFiles,
            DataSourceManager dataSourceManager,
            PageCache pageCache,
            Supplier<DatabaseHealth> databaseHealthSupplier )
    {
        this.storeDir = storeDir;
        this.storeFiles = storeFiles;
        this.dataSourceManager = dataSourceManager;
        this.pageCache = pageCache;
        this.databaseHealthSupplier = databaseHealthSupplier;
    }

    @Override
    public void init() throws Throwable
    {
        dataSourceManager.init();
    }

    @Override
    public synchronized void start() throws Throwable
    {
        storeId = readStoreIdFromDisk();
        dataSourceManager.start();
        started = true;
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        this.databaseHealth = null;
        dataSourceManager.stop();
        started = false;
    }

    @Override
    public void shutdown() throws Throwable
    {
        dataSourceManager.shutdown();
    }

    public synchronized StoreId storeId()
    {
        if ( started )
        {
            return storeId;
        }
        else
        {
            return readStoreIdFromDisk();
        }
    }

    private StoreId readStoreIdFromDisk()
    {
        try
        {
            File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
            org.neo4j.kernel.impl.store.StoreId kernelStoreId = MetaDataStore.getStoreId( pageCache, neoStoreFile );
            return new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(),
                    kernelStoreId.getUpgradeTime(), kernelStoreId.getUpgradeId() );
        }
        catch ( IOException e )
        {
            return null;
        }
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

    public void delete() throws IOException
    {
        storeFiles.delete( storeDir );
    }

    public boolean isEmpty() throws IOException
    {
        // TODO: Below doesn't work for an imported store. Need to check high-ids as well.
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        return readOnlyTransactionIdStore.getLastCommittedTransactionId() <= BASE_TX_ID;
    }

    @Override
    public StoreId get()
    {
        return storeId();
    }

    public File storeDir()
    {
        return storeDir;
    }

    public void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.moveTo( sourceDir, storeDir );
    }
}
