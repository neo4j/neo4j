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
package org.neo4j.bolt.runtime;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.kernel.monitoring.Monitors;

public class MetricsReportingBoltConnectionFactory implements BoltConnectionFactory
{
    private final Monitors monitors;
    private final BoltConnectionMetricsMonitor metricsMonitor;
    private final BoltConnectionFactory delegate;
    private final Clock clock;

    public MetricsReportingBoltConnectionFactory( Monitors monitors, BoltConnectionFactory delegate, Clock clock )
    {
        this.monitors = monitors;
        this.metricsMonitor = monitors.newMonitor( BoltConnectionMetricsMonitor.class );
        this.delegate = delegate;
        this.clock = clock;
    }

    @Override
    public BoltConnection newConnection( BoltChannel channel )
    {
        BoltConnection connection = delegate.newConnection( channel );

        if ( monitors.hasListeners( BoltConnectionMetricsMonitor.class ) )
        {
            return new MetricsReportingBoltConnection( connection, metricsMonitor, clock );
        }

        return connection;
    }

}
