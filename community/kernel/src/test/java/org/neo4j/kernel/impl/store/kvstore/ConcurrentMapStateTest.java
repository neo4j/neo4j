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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConcurrentMapStateTest
{

    private final ReadableState<?> store = mock( ReadableState.class );
    private final File file = mock( File.class );
    private final Lock lock = mock( Lock.class );

    @Test
    public void shouldCreateAnUpdaterForTheNextUnseenVersionUpdate() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        // when
        long updateVersion = 43;
        EntryUpdater<?> updater = state.updater( updateVersion, lock );

        // then
        // it does not blow up
        assertNotNull( updater );
        assertEquals( updateVersion, state.version() );
    }

    @Test
    public void shouldCreateAnUpdaterForAnUnseenVersionUpdateWithAGap() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        // when
        long updateVersion = 45;
        final EntryUpdater<?> updater = state.updater( updateVersion, lock );
        updater.close();

        // then
        // it does not blow up
        assertNotNull( updater );
        assertEquals( updateVersion, state.version() );
    }

    @Test
    public void shouldCreateAnUpdaterForMultipleVersionUpdatesInOrder() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        // when
        EntryUpdater<?> updater;

        long updateVersion = 43;
        updater = state.updater( updateVersion, lock );
        updater.close();

        updateVersion = 44;
        updater = state.updater( updateVersion, lock );
        updater.close();

        updateVersion = 45;
        updater = state.updater( updateVersion, lock );
        updater.close();

        // then
        // it does not blow up
        assertNotNull( updater );
        assertEquals( updateVersion, state.version() );
    }

    @Test
    public void shouldCreateAnUpdaterForMultipleVersionUpdatesNotInOrder() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        // when
        EntryUpdater<?> updater;

        long updateVersion = 45;
        updater = state.updater( updateVersion, lock );
        updater.close();

        updateVersion = 43;
        updater = state.updater( updateVersion, lock );
        updater.close();

        updateVersion = 44;
        updater = state.updater( updateVersion, lock );
        updater.close();

        // then
        // it does not blow up
        assertNotNull( updater );
        assertEquals( 45, state.version() );
    }

    @Test
    public void shouldUseEmptyUpdaterOnVersionLowerOrEqualToTheInitialVersion() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        // when
        EntryUpdater<?> updater = state.updater( initialVersion, lock );

        // expected
        assertEquals( "Empty updater should be used for version less or equal to initial",
                EntryUpdater.noUpdates(), updater );
    }
}
