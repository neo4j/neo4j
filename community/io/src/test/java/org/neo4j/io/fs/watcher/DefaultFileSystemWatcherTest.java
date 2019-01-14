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
package org.neo4j.io.fs.watcher;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultFileSystemWatcherTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private WatchService watchServiceMock = mock( WatchService.class );

    @Test
    public void fileWatchRegistrationIsIllegal() throws Exception
    {
        DefaultFileSystemWatcher watcher = createWatcher();

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Only directories can be registered to be monitored." );

        watcher.watch( new File( "notADirectory" ) );
    }

    @Test
    public void registerMultipleDirectoriesForMonitoring() throws Exception
    {
        try ( DefaultFileSystemWatcher watcher = new DefaultFileSystemWatcher(
                FileSystems.getDefault().newWatchService() ) )
        {
            File directory1 = testDirectory.directory( "test1" );
            File directory2 = testDirectory.directory( "test2" );
            WatchedResource watchedResource1 = watcher.watch( directory1 );
            WatchedResource watchedResource2 = watcher.watch( directory2 );
            assertNotSame( watchedResource1, watchedResource2 );
        }
    }

    @Test
    public void notifyListenersOnDeletion() throws InterruptedException
    {
        TestFileSystemWatcher watcher = createWatcher();
        AssertableFileEventListener listener1 = new AssertableFileEventListener();
        AssertableFileEventListener listener2 = new AssertableFileEventListener();

        watcher.addFileWatchEventListener( listener1 );
        watcher.addFileWatchEventListener( listener2 );

        TestWatchEvent<Path> watchEvent = new TestWatchEvent<>( ENTRY_DELETE, Paths.get( "file1" ) );
        TestWatchEvent<Path> watchEvent2 = new TestWatchEvent<>( ENTRY_DELETE, Paths.get( "file2" ) );
        TestWatchKey watchKey = new TestWatchKey( asList( watchEvent, watchEvent2 ) );

        prepareWatcher( watchKey );

        watch( watcher );

        listener1.assertDeleted( "file1" );
        listener1.assertDeleted( "file2" );
        listener2.assertDeleted( "file1" );
        listener2.assertDeleted( "file2" );
    }

    @Test
    public void notifyListenersOnModification() throws InterruptedException
    {
        TestFileSystemWatcher watcher = createWatcher();
        AssertableFileEventListener listener1 = new AssertableFileEventListener();
        AssertableFileEventListener listener2 = new AssertableFileEventListener();

        watcher.addFileWatchEventListener( listener1 );
        watcher.addFileWatchEventListener( listener2 );

        TestWatchEvent<Path> watchEvent = new TestWatchEvent<>( ENTRY_MODIFY, Paths.get( "a" ) );
        TestWatchEvent<Path> watchEvent2 = new TestWatchEvent<>( ENTRY_MODIFY, Paths.get( "b" ) );
        TestWatchEvent<Path> watchEvent3 = new TestWatchEvent<>( ENTRY_MODIFY, Paths.get( "c" ) );
        TestWatchKey watchKey = new TestWatchKey( asList( watchEvent, watchEvent2, watchEvent3 ) );

        prepareWatcher( watchKey );

        watch( watcher );

        listener1.assertModified( "a" );
        listener1.assertModified( "b" );
        listener1.assertModified( "c" );
        listener2.assertModified( "a" );
        listener2.assertModified( "b" );
        listener2.assertModified( "c" );
    }

    @Test
    public void stopWatchingAndCloseEverythingOnClosed() throws IOException
    {
        TestFileSystemWatcher watcher = createWatcher();
        watcher.close();

        verify( watchServiceMock ).close();
        assertTrue( watcher.isClosed() );
    }

    @Test
    public void skipEmptyEvent() throws InterruptedException
    {
        TestFileSystemWatcher watcher = createWatcher();

        AssertableFileEventListener listener = new AssertableFileEventListener();
        watcher.addFileWatchEventListener( listener );

        TestWatchEvent<String> event = new TestWatchEvent( ENTRY_MODIFY, null );
        TestWatchKey watchKey = new TestWatchKey( asList( event ) );

        prepareWatcher( watchKey );

        watch( watcher );

        listener.assertNoEvents();
    }

    private void prepareWatcher( TestWatchKey watchKey ) throws InterruptedException
    {
        when( watchServiceMock.take() ).thenReturn( watchKey )
                .thenThrow( InterruptedException.class );
    }

    private void watch( TestFileSystemWatcher watcher )
    {
        try
        {
            watcher.startWatching();
        }
        catch ( InterruptedException ignored )
        {
            // expected
        }
    }

    private TestFileSystemWatcher createWatcher()
    {
        return new TestFileSystemWatcher( watchServiceMock );
    }

    private static class TestFileSystemWatcher extends DefaultFileSystemWatcher
    {

        private boolean closed;

        TestFileSystemWatcher( WatchService watchService )
        {
            super( watchService );
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            closed = true;
        }

        public boolean isClosed()
        {
            return closed;
        }
    }

    private static class TestWatchKey implements WatchKey
    {
        private List<WatchEvent<?>> events;
        private boolean canceled;

        TestWatchKey( List<WatchEvent<?>> events )
        {
            this.events = events;
        }

        @Override
        public boolean isValid()
        {
            return false;
        }

        @Override
        public List<WatchEvent<?>> pollEvents()
        {
            return events;
        }

        @Override
        public boolean reset()
        {
            return false;
        }

        @Override
        public void cancel()
        {
            canceled = true;
        }

        @Override
        public Watchable watchable()
        {
            return null;
        }

        public boolean isCanceled()
        {
            return canceled;
        }
    }

    private static class TestWatchEvent<T> implements WatchEvent
    {

        private Kind<T> eventKind;
        private T fileName;

        TestWatchEvent( Kind<T> eventKind, T fileName )
        {
            this.eventKind = eventKind;
            this.fileName = fileName;
        }

        @Override
        public Kind kind()
        {
            return eventKind;
        }

        @Override
        public int count()
        {
            return 0;
        }

        @Override
        public T context()
        {
            return fileName;
        }
    }

    private static class AssertableFileEventListener implements FileWatchEventListener
    {
        private final List<String> deletedFileNames = new ArrayList<>();
        private final List<String> modifiedFileNames = new ArrayList<>();

        @Override
        public void fileDeleted( String fileName )
        {
            deletedFileNames.add( fileName );
        }

        @Override
        public void fileModified( String fileName )
        {
            modifiedFileNames.add( fileName );
        }

        void assertNoEvents()
        {
            assertThat( "Should not have any deletion events", deletedFileNames, empty() );
            assertThat( "Should not have any modification events", modifiedFileNames, empty() );
        }

        void assertDeleted( String fileName )
        {
            assertThat( "Was expected to find notification about deletion.", deletedFileNames, hasItem( fileName ) );
        }

        void assertModified( String fileName )
        {
            assertThat( "Was expected to find notification about modification.", modifiedFileNames,
                    hasItem( fileName ) );
        }
    }
}
