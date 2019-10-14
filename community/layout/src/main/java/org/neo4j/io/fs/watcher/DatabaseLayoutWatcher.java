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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.Objects.requireNonNull;

/**
 * File system fileWatcher that will remember all the files that it was asked to watch
 * and will register them in real file watcher during {@link #startWatching()} call.
 * When watcher will be stopped all registered resources will be closed.
 *
 * Described pattern allows to perform repeatable startWatching/stopWatching cycle for pre-configured set of files provided by {@link DatabaseLayout}.
 */
public class DatabaseLayoutWatcher extends LifecycleAdapter
{
    private final FileWatcher fileWatcher;
    private final DatabaseLayout databaseLayout;
    private final FileWatchEventListenerFactory listenerFactory;
    private final Set<File> filesToWatch = ConcurrentHashMap.newKeySet();
    private final Set<WatchedResource> watchedResources = ConcurrentHashMap.newKeySet();
    private FileWatchEventListener eventListener;

    public DatabaseLayoutWatcher( FileWatcher fileWatcher, DatabaseLayout databaseLayout, FileWatchEventListenerFactory listenerFactory )
    {
        requireNonNull( fileWatcher );
        requireNonNull( databaseLayout );
        requireNonNull( listenerFactory );
        this.fileWatcher = fileWatcher;
        this.databaseLayout = databaseLayout;
        this.listenerFactory = listenerFactory;
    }

    @Override
    public void start() throws Exception
    {
        watchDirectories();
        eventListener = listenerFactory.createListener( watchedResources );
        fileWatcher.addFileWatchEventListener( eventListener );
    }

    @Override
    public void stop() throws Exception
    {
        stopWatching();
        fileWatcher.removeFileWatchEventListener( eventListener );
    }

    private void watchDirectories()
    {
        Neo4jLayout layout = databaseLayout.getNeo4jLayout();
        watch( databaseLayout.databaseDirectory() );
        watch( databaseLayout.getTransactionLogsDirectory() );
        watch( layout.databasesDirectory() );
        watch( layout.transactionLogsRootDirectory() );
        watch( layout.homeDirectory() );
        startWatching();
    }

    private void watch( File file )
    {
        filesToWatch.add( file );
    }

    private void stopWatching()
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
    }

    private void startWatching()
    {
        for ( File fileToWatch : filesToWatch )
        {
            watchFile( fileToWatch );
        }
    }

    private void watchFile( File fileToWatch )
    {
        try
        {
            watchedResources.add( fileWatcher.watch( fileToWatch ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
