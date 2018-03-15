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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.time.Clock;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LocalDatabaseTest
{
    @Test
    public void availabilityGuardRaisedOnCreation()
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );
        LocalDatabase localDatabase = newLocalDatabase( guard );

        assertNotNull( localDatabase );
        assertDatabaseIsStoppedAndUnavailable( guard );
    }

    @Test
    public void availabilityGuardDroppedOnStart() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        LocalDatabase localDatabase = newLocalDatabase( guard );
        assertFalse( guard.isAvailable() );

        localDatabase.start();
        assertTrue( guard.isAvailable() );
    }

    @Test
    public void availabilityGuardRaisedOnStop() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        LocalDatabase localDatabase = newLocalDatabase( guard );
        assertFalse( guard.isAvailable() );

        localDatabase.start();
        assertTrue( guard.isAvailable() );

        localDatabase.stop();
        assertDatabaseIsStoppedAndUnavailable( guard );
    }

    @Test
    public void availabilityGuardRaisedOnStopForStoreCopy() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        LocalDatabase localDatabase = newLocalDatabase( guard );
        assertFalse( guard.isAvailable() );

        localDatabase.start();
        assertTrue( guard.isAvailable() );

        localDatabase.stopForStoreCopy();
        assertDatabaseIsStoppedForStoreCopyAndUnavailable( guard );
    }

    @Test
    public void availabilityGuardRaisedBeforeDataSourceManagerIsStopped() throws Throwable
    {
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );

        LocalDatabase localDatabase = newLocalDatabase( guard, dataSourceManager );
        localDatabase.stop();

        InOrder inOrder = inOrder( guard, dataSourceManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( dataSourceManager ).stop();
    }

    @Test
    public void availabilityGuardRaisedBeforeDataSourceManagerIsStoppedForStoreCopy() throws Throwable
    {
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );

        LocalDatabase localDatabase = newLocalDatabase( guard, dataSourceManager );
        localDatabase.stopForStoreCopy();

        InOrder inOrder = inOrder( guard, dataSourceManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( dataSourceManager ).stop();
    }

    @Test
    public void doNotRestartServicesIfAlreadyStarted() throws Throwable
    {
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        FileSystemWatcherService watcherService = mock( FileSystemWatcherService.class );
        LocalDatabase localDatabase = newLocalDatabase( newAvailabilityGuard(), dataSourceManager, watcherService );

        localDatabase.start();

        verify( dataSourceManager ).start();
        verify( watcherService ).start();
        reset( dataSourceManager );
        reset( watcherService );

        localDatabase.start();
        localDatabase.start();

        verify( dataSourceManager, never() ).start();
        verify( watcherService, never() ).start();
    }

    private static LocalDatabase newLocalDatabase( AvailabilityGuard availabilityGuard )
    {
        return newLocalDatabase( availabilityGuard, mock( DataSourceManager.class ) );
    }

    private static LocalDatabase newLocalDatabase( AvailabilityGuard availabilityGuard,
            DataSourceManager dataSourceManager )
    {
        return newLocalDatabase( availabilityGuard, dataSourceManager, FileSystemWatcherService.EMPTY_WATCHER );
    }

    private static LocalDatabase newLocalDatabase( AvailabilityGuard availabilityGuard,
            DataSourceManager dataSourceManager, FileSystemWatcherService fileWatcher )
    {
        return new LocalDatabase( new File( "." ), mock( StoreFiles.class ), mock( LogFiles.class ), dataSourceManager,
                () -> mock( DatabaseHealth.class ), fileWatcher, availabilityGuard,
                NullLogProvider.getInstance() );
    }

    private static AvailabilityGuard newAvailabilityGuard()
    {
        return new AvailabilityGuard( Clock.systemUTC(), NullLog.getInstance() );
    }

    private static void assertDatabaseIsStoppedAndUnavailable( AvailabilityGuard guard )
    {
        assertFalse( guard.isAvailable() );
        assertThat( guard.describeWhoIsBlocking(), containsString( "Database is stopped" ) );
    }

    private static void assertDatabaseIsStoppedForStoreCopyAndUnavailable( AvailabilityGuard guard )
    {
        assertFalse( guard.isAvailable() );
        assertThat( guard.describeWhoIsBlocking(), containsString( "Database is stopped to copy store" ) );
    }
}
