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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.WatchService;

import org.neo4j.io.fs.watcher.resource.WatchedResource;

/**
 * Watcher that allows receive notification about files modifications/removal for particular underlying file system.
 *
 * To be able to get notification users need to register resource they are interested in using
 * {@link #watch(File)} method call and add by adding {@link FileWatchEventListener listner} to be able to receive
 * status updates.
 *
 * @see WatchService
 */
public interface FileWatcher extends Closeable
{

    FileWatcher SILENT_WATCHER = new SilentFileWatcher();

    /**
     * Register provided directory in list of resources that we would like to watch and receive status modification
     * updates
     * @param file directory to be monitored for updates
     * @return
     * @throws IOException
     */
    WatchedResource watch( File file ) throws IOException;

    /**
     * Register listener to receive updates about registered resources.
     * @param listener listener to register
     */
    void addFileWatchEventListener( FileWatchEventListener listener );

    /**
     * Remove listener from a list of updates receivers.
     * @param listener listener to remove
     */
    void removeFileWatchEventListener( FileWatchEventListener listener );

    /**
     * Stop monitoring of registered directories
     */
    void stopWatching();

    /**
     * Start monitoring of registered directories.
     * This method we will wait for notification about registered resources, meaning that it will block thread where
     * it was called. If it is desired to start file watching as background task - watcher should be started in
     * separate thread.
     * Watching can be stopped by calling {@link #stopWatching()}.
     * @throws InterruptedException when interrupted while waiting for update notification to come
     */
    void startWatching() throws InterruptedException;
}
