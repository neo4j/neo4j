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
package org.neo4j.adversaries.watcher;

import java.io.File;
import java.io.IOException;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.fs.watcher.resource.WatchedResource;

/**
 * File watcher that injects additional failures using provided {@link Adversary}
 * and delegate all actual watching role to provided {@link FileWatcher}
 */
public class AdversarialFileWatcher implements FileWatcher
{
    private final FileWatcher fileWatcher;
    private final Adversary adversary;

    public AdversarialFileWatcher( FileWatcher fileWatcher, Adversary adversary )
    {
        this.fileWatcher = fileWatcher;
        this.adversary = adversary;
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        fileWatcher.close();
    }

    @Override
    public WatchedResource watch( File file ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return fileWatcher.watch( file );
    }

    @Override
    public void addFileWatchEventListener( FileWatchEventListener listener )
    {
        fileWatcher.addFileWatchEventListener( listener );
    }

    @Override
    public void removeFileWatchEventListener( FileWatchEventListener listener )
    {
        fileWatcher.removeFileWatchEventListener( listener );
    }

    @Override
    public void stopWatching()
    {
        fileWatcher.stopWatching();
    }

    @Override
    public void startWatching() throws InterruptedException
    {
        adversary.injectFailure( InterruptedException.class );
        fileWatcher.startWatching();
    }
}
