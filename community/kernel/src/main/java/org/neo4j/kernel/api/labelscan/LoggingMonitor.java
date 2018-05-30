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
package org.neo4j.kernel.api.labelscan;

import java.util.StringJoiner;

import org.neo4j.kernel.api.labelscan.LabelScanStore.Monitor;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.Format.duration;

/**
 * Logs about important events about {@link LabelScanStore} {@link Monitor}.
 */
public class LoggingMonitor extends Monitor.Adaptor
{
    private final Log log;

    public LoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void noIndex()
    {
        log.info( "No scan store found, this might just be first use. Preparing to rebuild." );
    }

    @Override
    public void notValidIndex()
    {
        log.warn( "Scan store could not be read. Preparing to rebuild." );
    }

    @Override
    public void rebuilding()
    {
        log.info( "Rebuilding scan store, this may take a while" );
    }

    @Override
    public void rebuilt( long roughNodeCount )
    {
        log.info( "Scan store rebuilt (roughly " + roughNodeCount + " nodes)" );
    }

    @Override
    public void recoveryCleanupRegistered()
    {
        log.info( "Scan store cleanup job registered" );
    }

    @Override
    public void recoveryCleanupStarted()
    {
        log.info( "Scan store cleanup job started" );
    }

    @Override
    public void recoveryCleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
    {
        StringJoiner joiner = new StringJoiner( ", ", "Scan store cleanup job finished: ", "" );
        joiner.add( "Number of pages visited: " + numberOfPagesVisited );
        joiner.add( "Number of cleaned crashed pointers: " + numberOfCleanedCrashPointers );
        joiner.add( "Time spent: " + duration( durationMillis ) );
        log.info( joiner.toString() );
    }

    @Override
    public void recoveryCleanupClosed()
    {
        log.info( "Scan store cleanup job closed" );
    }
}
