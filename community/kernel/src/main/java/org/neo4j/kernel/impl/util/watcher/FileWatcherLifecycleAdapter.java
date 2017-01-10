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

import java.util.concurrent.ThreadFactory;

import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Adapter that integrates file watching possibilities with platform lifecycle.
 * Monitoring will be started when corresponding life will be started and stopped as soon as life will be stopped.
 *
 * In case of restart new monitoring thread will be started.
 */
public class FileWatcherLifecycleAdapter extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final FileWatcher fileWatcher;
    private final FileSystemEventWatcher eventWatcher;
    private ThreadFactory fileWatchers;
    private Thread watcher;

    public FileWatcherLifecycleAdapter( JobScheduler jobScheduler, FileWatcher fileWatcher )
    {
        this.jobScheduler = jobScheduler;
        this.fileWatcher = fileWatcher;
        this.eventWatcher = new FileSystemEventWatcher();
    }

    @Override
    public void init() throws Throwable
    {
        fileWatchers = jobScheduler.threadFactory( JobScheduler.Groups.fileWatch );
    }

    @Override
    public void start() throws Throwable
    {
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
