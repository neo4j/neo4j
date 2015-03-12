package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
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
    public void shouldFailIfApplyingAVersionUpdateWithAVersionLowerOrEqualToTheInitialVersion() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        try
        {
            // when
            state.updater( initialVersion, lock );
            fail( "should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // then
            assertEquals( "Cannot apply update with given version " + initialVersion +
                          " when base version is " + initialVersion, ex.getMessage() );
        }
    }

    @Test
    public void shouldFailIfApplyingAVersionUpdateTwiceWithSameVersion() throws Exception
    {
        // given
        long initialVersion = 42;
        when( store.version() ).thenReturn( initialVersion );
        ConcurrentMapState<?> state = new ConcurrentMapState<>( store, file );

        EntryUpdater<?> updater;

        long updateVersion = 45;
        updater = state.updater( updateVersion, lock );
        updater.close();

        try
        {
            // when
            state.updater( updateVersion, lock );
            fail( "should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // then
            assertEquals( "Cannot apply update with given version " + updateVersion +
                          " when base version is " + initialVersion, ex.getMessage() );
        }
    }
}
