/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

/**
 * Listener that will print notification about deleted filename into internal log.
 */
public class DefaultFileDeletionEventListener implements FileWatchEventListener
{
    private final DatabaseLayout databaseLayout;
    private final Set<WatchedResource> watchedResources;
    private final Log internalLog;
    private final Predicate<String> fileNameFilter;

    DefaultFileDeletionEventListener( DatabaseLayout databaseLayout, Set<WatchedResource> watchedResources, LogService logService,
            Predicate<String> fileNameFilter )
    {
        this.databaseLayout = databaseLayout;
        this.watchedResources = watchedResources;
        this.internalLog = logService.getInternalLog( getClass() );
        this.fileNameFilter = fileNameFilter;
    }

    @Override
    public void fileDeleted( WatchKey key, String fileName )
    {
        if ( !fileNameFilter.test( fileName ) )
        {
            var watchedResource = getListenedResource( key );
            if ( watchedResource.isPresent() )
            {
                Path watchedFile = watchedResource.get().getWatchedFile();
                if ( isDatabaseDirectory( fileName, watchedFile ) )
                {
                    printWarning( fileName );
                }
                else if ( isFileInDatabaseDirectories( watchedFile ) )
                {
                    printWarning( fileName );
                }
            }
        }
    }

    private boolean isDatabaseDirectory( String fileName, Path watchedFile )
    {
        Neo4jLayout neo4jLayout = databaseLayout.getNeo4jLayout();
        return fileName.equals( databaseLayout.getDatabaseName() ) &&
                (neo4jLayout.databasesDirectory().equals( watchedFile ) ||
                 neo4jLayout.transactionLogsRootDirectory().equals( watchedFile ));
    }

    private boolean isFileInDatabaseDirectories( Path watchedFile )
    {
        return databaseLayout.databaseDirectory().equals( watchedFile ) ||
               databaseLayout.getTransactionLogsDirectory().equals( watchedFile );
    }

    private void printWarning( String fileName )
    {
        internalLog.error( "'%s' which belongs to the '%s' database was deleted while it was running.", fileName, databaseLayout.getDatabaseName() );
    }

    private Optional<WatchedResource> getListenedResource( WatchKey watchKey )
    {
        return watchedResources.stream().filter( resource -> watchKey.equals( resource.getWatchKey() ) ).findAny();
    }
}
