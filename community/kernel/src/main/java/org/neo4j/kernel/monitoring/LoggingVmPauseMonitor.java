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
package org.neo4j.kernel.monitoring;

import org.neo4j.logging.Log;
import org.neo4j.monitoring.VmPauseMonitor;

import static java.util.Objects.requireNonNull;

public class LoggingVmPauseMonitor implements VmPauseMonitor.Monitor
{
    private final Log log;

    public LoggingVmPauseMonitor( Log log )
    {
        this.log = requireNonNull( log );
    }

    @Override
    public void started()
    {
        log.debug( "Starting VM pause monitor" );
    }

    @Override
    public void stopped()
    {
        log.debug( "Stopping VM pause monitor" );
    }

    @Override
    public void interrupted()
    {
        log.debug( "VM pause monitor stopped" );
    }

    @Override
    public void failed( Exception e )
    {
        log.debug( "VM pause monitor failed", e );
    }

    @Override
    public void pauseDetected( VmPauseMonitor.VmPauseInfo info )
    {
        log.warn( "Detected VM stop-the-world pause: %s", info );
    }
}
