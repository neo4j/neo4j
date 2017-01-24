/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.watcher;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.kernel.impl.util.JobScheduler;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class FileSystemWatcherServiceFactoryTest
{

    private JobScheduler scheduler = mock( JobScheduler.class );

    @Test
    public void createDummyAdapterForSilentWatcher()
    {
        FileSystemWatcherService service =
                FileSystemWatcherServiceFactory.createFileSystemWatcherService( scheduler, FileWatcher.SILENT_WATCHER );
        assertSame( service, FileSystemWatcherService.EMPTY_WATCHER );
    }

    @Test
    public void createDefaultWatcherAdapter()
    {
        FileSystemWatcherService service =
                FileSystemWatcherServiceFactory.createFileSystemWatcherService( scheduler, mock( FileWatcher.class ) );
        assertThat( service, Matchers.instanceOf( DefaultFileSystemWatcherService.class ) );
    }
}
