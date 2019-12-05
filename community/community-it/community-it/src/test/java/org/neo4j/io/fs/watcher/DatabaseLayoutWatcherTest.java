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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.WatchKey;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Neo4jLayoutExtension
class DatabaseLayoutWatcherTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;
    private DatabaseLayoutWatcher watcher;
    private FileWatcher fileWatcher;
    private FileWatchEventListener eventListener;

    @BeforeEach
    void setUp() throws IOException
    {
        fileWatcher = mock( FileWatcher.class );
        when( fileWatcher.watch( any() ) ).then( (Answer<WatchedResource>) call -> new CountingWatchedResource( call.getArgument( 0 ) ) );
        eventListener = new FileWatchEventListener()
        {
        };

        FileWatchEventListenerFactory listenerFactory = set -> eventListener;
        watcher = new DatabaseLayoutWatcher( fileWatcher, databaseLayout, listenerFactory );
    }

    @Test
    void watchDatabaseDirectoryOnStart() throws Throwable
    {
        verifyNoInteractions( fileWatcher );

        watcher.start();

        verify( fileWatcher ).watch( databaseLayout.databaseDirectory() );
        verify( fileWatcher ).watch( testDirectory.homeDir() );
        verify( fileWatcher ).addFileWatchEventListener( eventListener );
    }

    @Test
    void stopWatchingDatabaseDirectoriesOnStop() throws Throwable
    {
        verifyNoInteractions( fileWatcher );

        watcher.start();
        watcher.stop();

        verify( fileWatcher ).removeFileWatchEventListener( eventListener );
        assertEquals( 5, CountingWatchedResource.COUNTER.get() );
    }

    private static class CountingWatchedResource implements WatchedResource
    {
        private static final AtomicLong COUNTER = new AtomicLong();
        private final File file;

        CountingWatchedResource( File file )
        {
            this.file = file;
        }

        @Override
        public WatchKey getWatchKey()
        {
            return null;
        }

        @Override
        public void close()
        {
            COUNTER.incrementAndGet();
        }
    }
}
