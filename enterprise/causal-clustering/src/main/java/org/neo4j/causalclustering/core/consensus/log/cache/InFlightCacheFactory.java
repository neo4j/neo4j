/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.cache;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.in_flight_cache_max_bytes;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.in_flight_cache_max_entries;

public class InFlightCacheFactory
{
    public static InFlightCache create( Config config, Monitors monitors )
    {
        return config.get( CausalClusteringSettings.in_flight_cache_type ).create( config, monitors );
    }

    public enum Type
    {
        NONE
                {
                    @Override
                    InFlightCache create( Config config, Monitors monitors )
                    {
                        return new VoidInFlightCache();
                    }
                },
        CONSECUTIVE
                {
                    @Override
                    InFlightCache create( Config config, Monitors monitors )
                    {
                        return new ConsecutiveInFlightCache( config.get( in_flight_cache_max_entries ), config.get( in_flight_cache_max_bytes ),
                                monitors.newMonitor( InFlightCacheMonitor.class ), false );
                    }
                },
        UNBOUNDED
                {
                    @Override
                    InFlightCache create( Config config, Monitors monitors )
                    {
                        return new UnboundedInFlightCache();
                    }
                };

        abstract InFlightCache create( Config config, Monitors monitors );
    }
}
