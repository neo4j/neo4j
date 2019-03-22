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

import java.nio.file.WatchKey;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

/**
 * Listener that will print notification about deleted filename into internal log.
 */
public class DefaultFileDeletionEventListener implements FileWatchEventListener
{
    private final DatabaseId databaseId;
    private final Set<WatchedResource> watchedResources;
    private final Log internalLog;
    private final Predicate<String> fileNameFilter;

    DefaultFileDeletionEventListener( DatabaseId databaseId, Set<WatchedResource> watchedResources, LogService logService, Predicate<String> fileNameFilter )
    {
        this.databaseId = databaseId;
        this.watchedResources = watchedResources;
        this.internalLog = logService.getInternalLog( getClass() );
        this.fileNameFilter = fileNameFilter;
    }

    @Override
    public void fileDeleted( WatchKey key, String fileName )
    {
        if ( isListenedResource( key ) && !fileNameFilter.test( fileName ) )
        {
            internalLog.error( "'%s' which belongs to the '%s' database was deleted while it was running.", fileName, databaseId.name() );
        }
    }

    private boolean isListenedResource( WatchKey watchKey )
    {
        return watchedResources.stream().map( WatchedResource::getWatchKey ).anyMatch( watchKey::equals );
    }
}
