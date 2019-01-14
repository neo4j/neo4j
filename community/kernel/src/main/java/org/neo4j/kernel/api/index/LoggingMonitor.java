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
package org.neo4j.kernel.api.index;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.util.StringJoiner;

import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.helpers.Format.duration;

public class LoggingMonitor implements IndexProvider.Monitor
{
    private final Log log;

    public LoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void failedToOpenIndex( StoreIndexDescriptor descriptor, String action, Exception cause )
    {
        log.error( "Failed to open index:" + descriptor.getId() + ". " + action, cause );
    }

    @Override
    public void recoveryCleanupRegistered( File indexFile, IndexDescriptor indexDescriptor )
    {
        log.info( "Schema index cleanup job registered: " + indexDescription( indexFile, indexDescriptor ) );
    }

    @Override
    public void recoveryCleanupStarted( File indexFile, IndexDescriptor indexDescriptor )
    {
        log.info( "Schema index cleanup job started: " + indexDescription( indexFile, indexDescriptor ) );
    }

    @Override
    public void recoveryCleanupFinished( File indexFile, IndexDescriptor indexDescriptor,
            long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
    {
        StringJoiner joiner =
                new StringJoiner( ", ", "Schema index cleanup job finished: " + indexDescription( indexFile, indexDescriptor ) + " ", "" );
        joiner.add( "Number of pages visited: " + numberOfPagesVisited );
        joiner.add( "Number of cleaned crashed pointers: " + numberOfCleanedCrashPointers );
        joiner.add( "Time spent: " + duration( durationMillis ) );
        log.info( joiner.toString() );
    }

    @Override
    public void recoveryCleanupClosed( File indexFile, IndexDescriptor indexDescriptor )
    {
        log.info( "Schema index cleanup job closed: " + indexDescription( indexFile, indexDescriptor ) );
    }

    @Override
    public void recoveryCleanupFailed( File indexFile, IndexDescriptor indexDescriptor, Throwable throwable )
    {
        log.info( String.format( "Schema index cleanup job failed: %s.%nCaused by: %s",
                indexDescription( indexFile, indexDescriptor ), ExceptionUtils.getStackTrace( throwable ) ) );
    }

    private String indexDescription( File indexFile, IndexDescriptor indexDescriptor )
    {
        return "descriptor=" + indexDescriptor.toString() + ", indexFile=" + indexFile.getAbsolutePath();
    }
}
