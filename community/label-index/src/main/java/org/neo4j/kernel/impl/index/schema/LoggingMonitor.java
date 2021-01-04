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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.StringJoiner;

import org.neo4j.common.EntityType;
import org.neo4j.logging.Log;

import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.Monitor;

/**
 * Logs about important events about {@link LabelScanStore} and {@link RelationshipTypeScanStore}.
 */
public class LoggingMonitor extends Monitor.Adaptor
{
    private final Log log;
    private final EntityType type;
    private final String lowerToken;
    private final String upperToken;

    public LoggingMonitor( Log log, EntityType type )
    {
        this.log = log;
        this.type = type;
        this.lowerToken = type == EntityType.NODE ? "label" : "relationship type";
        this.upperToken = type == EntityType.NODE ? "Label" : "Relationship type";
    }

    @Override
    public void noIndex()
    {
        log.info( "No %s index found, this might just be first use. Preparing to rebuild.", lowerToken );
    }

    @Override
    public void notValidIndex()
    {
        log.warn( "%s index could not be read. Preparing to rebuild.", upperToken );
    }

    @Override
    public void rebuilding()
    {
        log.info( "Rebuilding %s index, this may take a while", lowerToken );
    }

    @Override
    public void rebuilt( long roughEntityCount )
    {
        log.info( "%s index rebuilt (roughly %d %ss)", upperToken, roughEntityCount, type.name().toLowerCase() );
    }

    @Override
    public void recoveryCleanupRegistered()
    {
        log.info( "%s index cleanup job registered", upperToken );
    }

    @Override
    public void recoveryCleanupStarted()
    {
        log.info( "%s index cleanup job started", upperToken);
    }

    @Override
    public void recoveryCleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
    {
        StringJoiner joiner = new StringJoiner( ", ", upperToken + " index cleanup job finished: ", "" );
        joiner.add( "Number of pages visited: " + numberOfPagesVisited );
        joiner.add( "Number of tree nodes: " + numberOfTreeNodes );
        joiner.add( "Number of cleaned crashed pointers: " + numberOfCleanedCrashPointers );
        joiner.add( "Time spent: " + duration( durationMillis ) );
        log.info( joiner.toString() );
    }

    @Override
    public void recoveryCleanupClosed()
    {
        log.info( "%s index cleanup job closed", upperToken );
    }

    @Override
    public void recoveryCleanupFailed( Throwable throwable )
    {
        log.info( "%s index cleanup job failed.%nCaused by: %s", upperToken, ExceptionUtils.getStackTrace( throwable ) );
    }
}
