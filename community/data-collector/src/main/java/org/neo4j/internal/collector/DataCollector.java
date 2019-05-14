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
package org.neo4j.internal.collector;

import java.util.Collections;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.ValueMapper;

public class DataCollector implements AutoCloseable
{
    final Kernel kernel;
    final JobScheduler jobScheduler;
    final ValueMapper.JavaMapper valueMapper;
    final QueryCollector queryCollector;

    DataCollector( Kernel kernel,
                   JobScheduler jobScheduler,
                   Monitors monitors,
                   ValueMapper.JavaMapper valueMapper,
                   Config config )
    {
        this.kernel = kernel;
        this.jobScheduler = jobScheduler;
        this.valueMapper = valueMapper;
        this.queryCollector = new QueryCollector( jobScheduler,
                                                  config.get( GraphDatabaseSettings.data_collector_max_recent_query_count ),
                                                  config.get( GraphDatabaseSettings.data_collector_max_query_text_size ) );
        try
        {
            this.queryCollector.collect( Collections.emptyMap() );
        }
        catch ( InvalidArgumentsException e )
        {
            throw new IllegalStateException( "An empty config cannot be invalid", e );
        }
        monitors.addMonitorListener( queryCollector );
    }

    @Override
    public void close()
    {
        // intended to eventually be used to stop any ongoing collection
    }
}
