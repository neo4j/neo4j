/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.StringJoiner;

import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.Format.duration;

public class LoggingMonitor implements SchemaIndexProvider.Monitor
{
    private final Log log;

    public LoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void failedToOpenIndex( long indexId, IndexDescriptor indexDescriptor, String action, Exception cause )
    {
        log.error( "Failed to open index:" + indexId + ". " + action, cause );
    }

    @Override
    public void recoveryCleanupRegistered( long indexId, IndexDescriptor indexDescriptor )
    {
        log.info( "Schema index cleanup job registered: " + indexDescription( indexId, indexDescriptor ) );
    }

    @Override
    public void recoveryCleanupStarted( long indexId, IndexDescriptor indexDescriptor )
    {
        log.info( "Schema index cleanup job started: " + indexDescription( indexId, indexDescriptor ) );
    }

    @Override
    public void recoveryCleanupFinished( long indexId, IndexDescriptor indexDescriptor,
            long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
    {
        StringJoiner joiner =
                new StringJoiner( ", ", "Schema index cleanup job finished: " + indexDescription( indexId, indexDescriptor ) + " ", "" );
        joiner.add( "Number of pages visited: " + numberOfPagesVisited );
        joiner.add( "Number of cleaned crashed pointers: " + numberOfCleanedCrashPointers );
        joiner.add( "Time spent: " + duration( durationMillis ) );
        log.info( joiner.toString() );
    }

    @Override
    public void recoveryCleanupClosed( long indexId, IndexDescriptor descriptor )
    {
        log.info( "Schema index cleanup job closed: " + indexDescription( indexId, descriptor ) );
    }

    @Override
    public void recoveryCleanupFailed( long indexId, IndexDescriptor descriptor, Throwable throwable )
    {
        log.info( "Schema index cleanup job failed: " + indexDescription( indexId, descriptor ) + ".\n" +
                "Caused by: " + ExceptionUtils.getStackTrace( throwable ) );
    }

    private String indexDescription( long indexId, IndexDescriptor indexDescriptor )
    {
        return "indexId: " + indexId + " descriptor: " + indexDescriptor.toString();
    }
}
