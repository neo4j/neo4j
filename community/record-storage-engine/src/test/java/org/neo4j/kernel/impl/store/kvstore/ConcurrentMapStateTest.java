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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.counts.TxVersionContextSupplier;
import org.neo4j.util.concurrent.Runnables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConcurrentMapStateTest
{
    private final ReadableState<String> store = mock( ReadableState.class );
    private final File file = mock( File.class );
    private final Lock lock = mock( Lock.class );

    @BeforeEach
    void setUp()
    {
        KeyFormat keyFormat = mock( KeyFormat.class );
        when( keyFormat.valueSize() ).thenReturn( Long.BYTES );
        when( store.keyFormat() ).thenReturn( keyFormat );
    }

    @Test
    void shouldCreateAnUpdaterForTheNextUnseenVersionUpdate()
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = createMapState();

        // when
        long updateVersion = 43;
        EntryUpdater<?> updater = state.updater( updateVersion, lock );

        // then
        // it does not blow up
        assertNotNull( updater );
        assertEquals( updateVersion, state.version() );
    }

    @Test
    void shouldCreateAnUpdaterForAnUnseenVersionUpdateWithAGap()
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = createMapState();

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
    void shouldCreateAnUpdaterForMultipleVersionUpdatesInOrder()
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = createMapState();

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
    void shouldCreateAnUpdaterForMultipleVersionUpdatesNotInOrder()
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = createMapState();

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
    void shouldUseEmptyUpdaterOnVersionLowerOrEqualToTheInitialVersion()
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = createMapState();

        // when
        EntryUpdater<?> updater = state.updater( initialVersion, lock );

        // expected
        assertEquals(
            EntryUpdater.noUpdates(), updater, "Empty updater should be used for version less or equal to initial" );
    }

    @Test
    void markDirtyVersionLookupOnKeyUpdate() throws IOException
    {
        long updaterVersionTxId = 25;
        long lastClosedTxId = 20;
        TxVersionContextSupplier versionContextSupplier = new TxVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTxId );
        ConcurrentMapState<String> mapState = createMapState( versionContextSupplier );
        VersionContext versionContext = versionContextSupplier.getVersionContext();
        try ( EntryUpdater<String> updater = mapState.updater( updaterVersionTxId, lock ) )
        {
            updater.apply( "a", new SimpleValueUpdate( 1 ) );
            updater.apply( "b", new SimpleValueUpdate( 2 ) );
        }

        assertEquals( updaterVersionTxId, mapState.version() );
        versionContext.initRead();
        mapState.lookup( "a", new EmptyValueSink() );
        assertTrue( versionContext.isDirty() );
    }

    @Test
    void markDirtyVersionLookupOnKeyReset() throws IOException
    {
        long updaterVersionTxId = 25;
        long lastClosedTxId = 20;
        when( store.version() ).thenReturn( updaterVersionTxId );
        TxVersionContextSupplier versionContextSupplier = new TxVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTxId );
        VersionContext versionContext = versionContextSupplier.getVersionContext();

        ConcurrentMapState<String> mapState = createMapState( versionContextSupplier );

        versionContext.initRead();
        mapState.resettingUpdater( lock, Runnables.EMPTY_RUNNABLE ).apply( "a", new SimpleValueUpdate( 1 ) );
        mapState.lookup( "a", new EmptyValueSink() );
        assertTrue( versionContext.isDirty() );
    }

    @Test
    void doNotMarkVersionAsDirtyOnAnotherKeyUpdate() throws IOException
    {
        long updaterVersionTxId = 25;
        long lastClosedTxId = 20;
        TxVersionContextSupplier versionContextSupplier = new TxVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTxId );
        ConcurrentMapState<String> mapState = createMapState( versionContextSupplier );
        VersionContext versionContext = versionContextSupplier.getVersionContext();
        try ( EntryUpdater<String> updater = mapState.updater( updaterVersionTxId, lock ) )
        {
            updater.apply( "b", new SimpleValueUpdate( 2 ) );
        }

        assertEquals( updaterVersionTxId, mapState.version() );
        versionContext.initRead();
        mapState.lookup( "a", new EmptyValueSink() );
        assertFalse( versionContext.isDirty() );
    }

    private ConcurrentMapState<String> createMapState()
    {
        return createMapState( EmptyVersionContextSupplier.EMPTY );
    }

    private ConcurrentMapState<String> createMapState( VersionContextSupplier versionContextSupplier )
    {
        return new ConcurrentMapState<>( store, file, versionContextSupplier );
    }

    private static class SimpleValueUpdate implements ValueUpdate
    {
        private final long value;

        SimpleValueUpdate( long value )
        {
            this.value = value;
        }

        @Override
        public void update( WritableBuffer target )
        {
            target.putLong( 0, value );
        }
    }

    private static class EmptyValueSink extends ValueSink
    {
        @Override
        protected void value( ReadableBuffer value )
        {

        }
    }
}
