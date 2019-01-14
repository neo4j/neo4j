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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;

import java.time.Clock;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

/**
 * Serves as an entry point for throttling of transport related resources. Currently only
 * write operations are throttled based on pending buffered data. If there will be new types
 * of throttles (number of active channels, reads, etc.) they should be added and registered
 * through this class.
 */
public class TransportThrottleGroup
{
    public static final TransportThrottleGroup NO_THROTTLE = new TransportThrottleGroup();

    private final TransportThrottle writeThrottle;

    private TransportThrottleGroup()
    {
        this.writeThrottle = NoOpTransportThrottle.INSTANCE;
    }

    public TransportThrottleGroup( Config config, Clock clock )
    {
        this.writeThrottle = createWriteThrottle( config, clock );
    }

    public TransportThrottle writeThrottle()
    {
        return writeThrottle;
    }

    public void install( Channel channel )
    {
        writeThrottle.install( channel );
    }

    public void uninstall( Channel channel )
    {
        writeThrottle.uninstall( channel );
    }

    private static TransportThrottle createWriteThrottle( Config config, Clock clock )
    {
        if ( config.get( GraphDatabaseSettings.bolt_outbound_buffer_throttle) )
        {
            return new TransportWriteThrottle( config.get( GraphDatabaseSettings.bolt_outbound_buffer_throttle_low_water_mark ),
                    config.get( GraphDatabaseSettings.bolt_outbound_buffer_throttle_high_water_mark ), clock,
                    config.get( GraphDatabaseSettings.bolt_outbound_buffer_throttle_max_duration ) );
        }

        return NoOpTransportThrottle.INSTANCE;
    }

    private static class NoOpTransportThrottle implements TransportThrottle
    {
        private static final TransportThrottle INSTANCE = new NoOpTransportThrottle();

        @Override
        public void install( Channel channel )
        {

        }

        @Override
        public void acquire( Channel channel )
        {

        }

        @Override
        public void release( Channel channel )
        {

        }

        @Override
        public void uninstall( Channel channel )
        {

        }
    }
}
