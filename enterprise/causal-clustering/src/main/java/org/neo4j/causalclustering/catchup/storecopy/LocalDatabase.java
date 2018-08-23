/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;

public class LocalDatabase implements Lifecycle
{
    private static final AvailabilityRequirement NOT_STOPPED = new DescriptiveAvailabilityRequirement( "Database is stopped" );
    private static final AvailabilityRequirement NOT_COPYING_STORE =
            new DescriptiveAvailabilityRequirement( "Database is stopped to copy store from another cluster member" );

    private final DatabaseLayout databaseLayout;

    private final StoreFiles storeFiles;
    private final DataSourceManager dataSourceManager;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final AvailabilityGuard availabilityGuard;
    private final Log log;

    private volatile StoreId storeId;
    private volatile DatabaseHealth databaseHealth;
    private volatile AvailabilityRequirement currentRequirement;

    private volatile TransactionCommitProcess localCommit;
    private final LogFiles logFiles;

    public LocalDatabase( DatabaseLayout databaseLayout,
            StoreFiles storeFiles,
            LogFiles logFiles,
            DataSourceManager dataSourceManager,
            Supplier<DatabaseHealth> databaseHealthSupplier,
            AvailabilityGuard availabilityGuard,
            LogProvider logProvider )
    {
        this.databaseLayout = databaseLayout;
        this.storeFiles = storeFiles;
        this.logFiles = logFiles;
        this.dataSourceManager = dataSourceManager;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog( getClass() );
        raiseAvailabilityGuard( NOT_STOPPED );
    }

    @Override
    public void init()
    {
        dataSourceManager.init();
    }

    @Override
    public synchronized void start()
    {
        if ( isAvailable() )
        {
            return;
        }
        storeId = readStoreIdFromDisk();
        log.info( "Starting with storeId: " + storeId );

        dataSourceManager.start();

        dropAvailabilityGuard();
    }

    @Override
    public void stop() throws Throwable
    {
        stopWithRequirement( NOT_STOPPED );
    }

    /**
     * Stop database to perform a store copy. This will raise {@link DatabaseAvailabilityGuard} with
     * a more friendly blocking requirement.
     *
     * @throws Throwable if any of the components are unable to stop.
     */
    public void stopForStoreCopy() throws Throwable
    {
        stopWithRequirement( NOT_COPYING_STORE );
    }

    public boolean isAvailable()
    {
        return currentRequirement == null;
    }

    @Override
    public void shutdown()
    {
        dataSourceManager.shutdown();
    }

    public synchronized StoreId storeId()
    {
        if ( isAvailable() )
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
            return storeFiles.readStoreId( databaseLayout );
        }
        catch ( IOException e )
        {
            log.error( "Failure reading store id", e );
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
        storeFiles.delete( databaseLayout.databaseDirectory(), logFiles );
    }

    public boolean isEmpty() throws IOException
    {
        Set<File> filesToLookFor = databaseLayout.storeFiles();
        return storeFiles.isEmpty( databaseLayout.databaseDirectory(), filesToLookFor );
    }

    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.delete( databaseLayout.databaseDirectory(), logFiles );
        storeFiles.moveTo( sourceDir, databaseLayout.databaseDirectory(), logFiles );
    }

    public NeoStoreDataSource dataSource()
    {
        return dataSourceManager.getDataSource();
    }

    /**
     * Called by the DataSourceManager during start.
     */
    public void registerCommitProcessDependencies( TransactionAppender appender, StorageEngine applier )
    {
        localCommit = new TransactionRepresentationCommitProcess( appender, applier );
    }

    public TransactionCommitProcess getCommitProcess()
    {
        return localCommit;
    }

    private synchronized void stopWithRequirement( AvailabilityRequirement requirement ) throws Throwable
    {
        log.info( "Stopping, reason: " + requirement.description() );
        raiseAvailabilityGuard( requirement );
        databaseHealth = null;
        localCommit = null;
        dataSourceManager.stop();
    }

    private void raiseAvailabilityGuard( AvailabilityRequirement requirement )
    {
        // it is possible for the local database to be created and stopped right after that to perform a store copy
        // in this case we need to impose new requirement and drop the old one
        availabilityGuard.require( requirement );
        if ( currentRequirement != null )
        {
            dropAvailabilityGuard();
        }
        currentRequirement = requirement;
    }

    private void dropAvailabilityGuard()
    {
        availabilityGuard.fulfill( currentRequirement );
        currentRequirement = null;
    }
}
