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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.watcher.resource.WatchedResource;

/**
 * File system delegate that will remember all the files that it was asked to watch
 * and will register them in real delegate during {@link #startWatching()} call.
 * When delegate will be stopped all registered resources will be closed and delegate delegate will be stopped.
 *
 * Described pattern allows to perform repeatable startWatching/stopWatching cycle for pre-configured set of files.
 */
public class RestartableFileSystemWatcher implements FileWatcher
{
    private FileWatcher delegate;
    private Set<File> filesToWatch = Collections.newSetFromMap( new ConcurrentHashMap<>() );
    private Set<WatchedResource> watchedResources = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public RestartableFileSystemWatcher( FileWatcher delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public WatchedResource watch( File file )
    {
        filesToWatch.add( file );
        return WatchedResource.EMPTY;
    }

    @Override
    public void addFileWatchEventListener( FileWatchEventListener listener )
    {
        delegate.addFileWatchEventListener( listener );
    }

    @Override
    public void removeFileWatchEventListener( FileWatchEventListener listener )
    {
        delegate.removeFileWatchEventListener( listener );
    }

    @Override
    public void stopWatching()
    {
        try
        {
            IOUtils.closeAll( watchedResources );
            watchedResources.clear();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            delegate.stopWatching();
        }
    }

    @Override
    public void startWatching() throws InterruptedException
    {
        for ( File fileToWatch : filesToWatch )
        {
            watchFile( fileToWatch );
        }
        delegate.startWatching();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

    private void watchFile( File fileToWatch )
    {
        try
        {
            watchedResources.add( delegate.watch( fileToWatch ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
