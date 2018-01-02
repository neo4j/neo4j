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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.NeoStoreDataSource;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataSourceManagerTest
{
    @Test
    public void shouldCallListenersOnStart() throws Throwable
    {
        // given
        DataSourceManager manager = new DataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.register( mock( NeoStoreDataSource.class ) );
        manager.addListener( listener );

        // when
        manager.start();

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    public void shouldCallListenersWhenAddedIfManagerAlreadyStarted() throws Throwable
    {
        // given
        DataSourceManager manager = new DataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.register( mock( NeoStoreDataSource.class ) );
        manager.start();

        // when
        manager.addListener( listener );

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    public void shouldCallListenersOnDataSourceRegistrationIfManagerAlreadyStarted() throws Throwable
    {
        // given
        DataSourceManager manager = new DataSourceManager();
        DataSourceManager.Listener listener = mock( DataSourceManager.Listener.class );
        manager.addListener( listener );
        manager.start();

        // when
        manager.register( mock( NeoStoreDataSource.class ) );

        // then
        verify( listener ).registered( any( NeoStoreDataSource.class ) );
    }

    @Test
    public void shouldSupportMultipleStartStopCycles() throws Throwable
    {
        // given
        DataSourceManager manager = new DataSourceManager();
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
}
