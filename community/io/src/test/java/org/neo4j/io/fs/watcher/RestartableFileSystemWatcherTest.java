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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.watcher.resource.WatchedResource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestartableFileSystemWatcherTest
{

    private FileWatcher delegate = mock( FileWatcher.class );
    private RestartableFileSystemWatcher watcher = new RestartableFileSystemWatcher( delegate );

    @Test
    public void delegateListenersCallToRealWatcher()
    {
        FileWatchEventListener listener = mock( FileWatchEventListener.class );

        watcher.addFileWatchEventListener( listener );
        verify( delegate ).addFileWatchEventListener( listener );

        watcher.removeFileWatchEventListener( listener );
        verify( delegate ).removeFileWatchEventListener( listener );
    }

    @Test
    public void closeDelegateOnClose() throws IOException
    {
        watcher.close();
        verify( delegate ).close();
    }

    @Test
    public void startStopFileWatchingCycle() throws IOException, InterruptedException
    {
        File file1 = mock( File.class );
        File file2 = mock( File.class );
        WatchedResource resource1 = mock( WatchedResource.class );
        WatchedResource resource2 = mock( WatchedResource.class );
        watcher.watch( file1 );
        watcher.watch( file2 );

        when( delegate.watch( file1 ) ).thenReturn( resource1 );
        when( delegate.watch( file2 ) ).thenReturn( resource2 );

        int invocations = 100;
        for ( int i = 0; i < invocations; i++ )
        {
            startStopWatching();
        }

        verify( delegate, times( invocations ) ).watch( file1 );
        verify( delegate, times( invocations ) ).watch( file2 );
        verify( delegate, times( invocations ) ).startWatching();
        verify( delegate, times( invocations ) ).stopWatching();

        verify( resource1, times( invocations ) ).close();
        verify( resource2, times( invocations ) ).close();
    }

    private void startStopWatching() throws InterruptedException
    {
        watcher.startWatching();
        watcher.stopWatching();
    }
}
