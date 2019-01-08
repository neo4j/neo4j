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

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.io.fs.watcher.resource.WatchedFile;
import org.neo4j.io.fs.watcher.resource.WatchedResource;

import static java.lang.String.format;

/**
 * File watcher that monitors registered directories state using possibilities provided by {@link WatchService}.
 *
 * Safe to be used from multiple threads
 */
public class DefaultFileSystemWatcher implements FileWatcher
{
    private static final WatchEvent.Kind[] OBSERVED_EVENTS =
            new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY};
    private final WatchService watchService;
    private final List<FileWatchEventListener> listeners = new CopyOnWriteArrayList<>();
    private volatile boolean watch;

    public DefaultFileSystemWatcher( WatchService watchService )
    {
        this.watchService = watchService;
    }

    @Override
    public WatchedResource watch( File file ) throws IOException
    {
        if ( !file.isDirectory() )
        {
            throw new IllegalArgumentException( format( "File `%s` is not a directory. Only directories can be " +
                    "registered to be monitored.", file.getCanonicalPath() ) );
        }
        WatchKey watchKey = file.toPath().register( watchService, OBSERVED_EVENTS, SensitivityWatchEventModifier.HIGH );
        return new WatchedFile( watchKey );
    }

    @Override
    public void startWatching() throws InterruptedException
    {
        watch = true;
        while ( watch )
        {
            WatchKey key = watchService.take();
            if ( key != null )
            {
                List<WatchEvent<?>> watchEvents = key.pollEvents();
                for ( WatchEvent<?> watchEvent : watchEvents )
                {
                    WatchEvent.Kind<?> kind = watchEvent.kind();
                    if ( StandardWatchEventKinds.ENTRY_MODIFY == kind )
                    {
                        notifyAboutModification( watchEvent );
                    }
                    if ( StandardWatchEventKinds.ENTRY_DELETE == kind )
                    {
                        notifyAboutDeletion( watchEvent );
                    }
                }
                key.reset();
            }
        }
    }

    @Override
    public void stopWatching()
    {
        watch = false;
    }

    @Override
    public void addFileWatchEventListener( FileWatchEventListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeFileWatchEventListener( FileWatchEventListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void close() throws IOException
    {
        stopWatching();
        watchService.close();
    }

    private void notifyAboutModification( WatchEvent<?> watchEvent )
    {
        String context = getContext( watchEvent );
        if ( StringUtils.isNotEmpty( context ) )
        {
            for ( FileWatchEventListener listener : listeners )
            {
                listener.fileModified( context );
            }
        }
    }

    private void notifyAboutDeletion( WatchEvent<?> watchEvent )
    {
        String context = getContext( watchEvent );
        if ( StringUtils.isNotEmpty( context ) )
        {
            for ( FileWatchEventListener listener : listeners )
            {
                listener.fileDeleted( context );
            }
        }
    }

    private String getContext( WatchEvent<?> watchEvent )
    {
        return Objects.toString( watchEvent.context(), StringUtils.EMPTY );
    }
}
