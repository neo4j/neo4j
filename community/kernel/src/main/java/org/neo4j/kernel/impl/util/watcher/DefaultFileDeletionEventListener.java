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

import org.neo4j.io.fs.watcher.event.FileWatchEventListenerAdapter;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.logging.Log;

import static java.lang.String.format;

/**
 * Listener that will print notification about deleted filename into internal log.
 */
public class DefaultFileDeletionEventListener extends FileWatchEventListenerAdapter
{

    private static final String EXTENSION_SEPARATOR = ".";
    private final Log internalLog;

    public DefaultFileDeletionEventListener( LogService logService )
    {
        this.internalLog = logService.getInternalLog( getClass() );
    }

    @Override
    public void fileDeleted( String fileName )
    {
        if ( isMonitoredFile( fileName ) )
        {
            internalLog.info( format( "Store %s '%s' was deleted while database was running.", getFileType( fileName ),
                    fileName ) );
        }
    }

    private static boolean isMonitoredFile( String fileName )
    {
        return !fileName.startsWith( PhysicalLogFile.DEFAULT_NAME );
    }

    private static String getFileType( String fileName )
    {
        return isFile( fileName ) ? "file" : "directory";
    }

    private static boolean isFile( String fileName )
    {
        return fileName.startsWith( MetaDataStore.DEFAULT_NAME ) || fileName.contains( EXTENSION_SEPARATOR );
    }

}
