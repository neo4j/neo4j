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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( TestDirectoryExtension.class )
class DataSourceManagerTest
{

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldCallListenersOnStart()
    {
        // given
        DataSourceManager manager = createDataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.register( mock( NeoStoreDataSource.class ) );
        manager.addListener( listener );

        // when
        manager.start();

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    void shouldCallListenersWhenAddedIfManagerAlreadyStarted()
    {
        // given
        DataSourceManager manager = createDataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.register( mock( NeoStoreDataSource.class ) );
        manager.start();

        // when
        manager.addListener( listener );

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    void shouldCallListenersOnDataSourceRegistrationIfManagerAlreadyStarted()
    {
        // given
        DataSourceManager manager = createDataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.addListener( listener );
        manager.start();

        // when
        manager.register( mock( NeoStoreDataSource.class ) );

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    void shouldSupportMultipleStartStopCycles() throws Throwable
    {
        // given
        DataSourceManager manager = createDataSourceManager();
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        manager.register( dataSource );
        manager.init();

        // when
        manager.start();
        manager.stop();
        manager.start();

        // then
        verify( dataSource, times( 2 ) ).start();
    }

    @Test
    void provideAccessOnlyToActiveDatabase()
    {
        DataSourceManager manager = createDataSourceManager();
        NeoStoreDataSource dataSource1 = mock( NeoStoreDataSource.class );
        NeoStoreDataSource dataSource2 = mock( NeoStoreDataSource.class );
        when( dataSource1.getDatabaseLayout() ).thenReturn( testDirectory.databaseLayout() );
        when( dataSource2.getDatabaseLayout() ).thenReturn( testDirectory.databaseLayout( "somethingElse" ) );
        manager.register( dataSource1 );
        manager.register( dataSource2 );

        assertEquals(dataSource1, manager.getDataSource() );
    }

    @Test
    void illegalStateWhenActiveDatabaseNotFound()
    {
        DataSourceManager manager = createDataSourceManager();
        assertThrows( IllegalStateException.class, manager::getDataSource );
    }

    private static DataSourceManager createDataSourceManager()
    {
        return new DataSourceManager( Config.defaults() );
    }
}
