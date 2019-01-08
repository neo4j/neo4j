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

import java.util.function.Predicate;

import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static java.lang.String.format;

/**
 * Listener that will print notification about deleted filename into internal log.
 */
public class DefaultFileDeletionEventListener implements FileWatchEventListener
{

    private final Log internalLog;
    private final Predicate<String> fileNameFilter;

    public DefaultFileDeletionEventListener( LogService logService, Predicate<String> fileNameFilter )
    {
        this.internalLog = logService.getInternalLog( getClass() );
        this.fileNameFilter = fileNameFilter;
    }

    @Override
    public void fileDeleted( String fileName )
    {
        if ( !fileNameFilter.test( fileName ) )
        {
            internalLog.error( format( "'%s' which belongs to the store was deleted while database was running.",
                    fileName ) );
        }
    }
}
