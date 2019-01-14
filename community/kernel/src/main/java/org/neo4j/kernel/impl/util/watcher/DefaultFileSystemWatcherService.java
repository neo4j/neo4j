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
package org.neo4j.kernel.impl.util.watcher;

import java.util.concurrent.ThreadFactory;

import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.scheduler.JobScheduler;

/**
 * Factory used for construction of proper adaptor for available {@link FileWatcher}.
 * In case if silent matcher is used dummy adapter will be used, otherwise will use default wrapper that will bind
 * monitoring cycles to corresponding lifecycle phases.
 */
public class DefaultFileSystemWatcherService implements FileSystemWatcherService
{
    private final JobScheduler jobScheduler;
    private final FileWatcher fileWatcher;
    private final FileSystemEventWatcher eventWatcher;
    private ThreadFactory fileWatchers;
    private Thread watcher;

    public DefaultFileSystemWatcherService( JobScheduler jobScheduler, FileWatcher fileWatcher )
    {
        this.jobScheduler = jobScheduler;
        this.fileWatcher = fileWatcher;
        this.eventWatcher = new FileSystemEventWatcher();
    }

    @Override
    public void init()
    {
        fileWatchers = jobScheduler.threadFactory( JobScheduler.Groups.fileWatch );
    }

    @Override
    public void start()
    {
        assert watcher == null;
        watcher = fileWatchers.newThread( eventWatcher );
        watcher.start();
    }

    @Override
    public void stop() throws Throwable
    {
        eventWatcher.stopWatching();
        if ( watcher != null )
        {
            watcher.interrupt();
            watcher.join();
            watcher = null;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        fileWatcher.close();
    }

    @Override
    public FileWatcher getFileWatcher()
    {
        return fileWatcher;
    }

    private class FileSystemEventWatcher implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                fileWatcher.startWatching();
            }
            catch ( InterruptedException ignored )
            {
            }
        }

        void stopWatching()
        {
            fileWatcher.stopWatching();
        }
    }
}
