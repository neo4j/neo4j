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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

public class DataCollector extends LifecycleAdapter
{
    private final Database database;
    private final QueryCollector queryCollector;

    public DataCollector( Database database, JobScheduler jobScheduler, Monitors monitors, Config config )
    {
        this.database = database;
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
    public void stop()
    {
        queryCollector.doStop();
    }

    public Kernel getKernel()
    {
        return database.getKernel();
    }

    QueryCollector getQueryCollector()
    {
        return queryCollector;
    }
}
