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
package org.neo4j.kernel.impl.query;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

public abstract class QueryEngineProvider
{
    protected abstract QueryExecutionEngine createEngine( Dependencies deps,
                                                          GraphDatabaseAPI graphAPI,
                                                          boolean isSystemDatabase,
                                                          SPI spi );

    protected abstract int enginePriority();

    public static QueryExecutionEngine initialize( Dependencies deps,
                                                   GraphDatabaseAPI graphAPI,
                                                   QueryEngineProvider provider,
                                                   boolean isSystemDatabase,
                                                   SPI spi )
    {
        if ( provider == null )
        {
            return noEngine();
        }
        QueryExecutionEngine engine = provider.createEngine( deps, graphAPI, isSystemDatabase, spi );
        return deps.satisfyDependency( engine );
    }

    public static QueryExecutionEngine noEngine()
    {
        return NoQueryEngine.INSTANCE;
    }

    public interface SPI
    {
        LogProvider logProvider();

        Monitors monitors();

        JobScheduler jobScheduler();

        LifeSupport lifeSupport();

        Kernel kernel();

        Config config();
    }

    public static SPI spi( LogProvider logProvider,
                           Monitors monitors,
                           JobScheduler jobScheduler,
                           LifeSupport lifeSupport,
                           Kernel kernel,
                           Config config )
    {
        return new SPI()
        {
            @Override
            public LogProvider logProvider()
            {
                return logProvider;
            }

            @Override
            public Monitors monitors()
            {
                return monitors;
            }

            @Override
            public JobScheduler jobScheduler()
            {
                return jobScheduler;
            }

            @Override
            public LifeSupport lifeSupport()
            {
                return lifeSupport;
            }

            @Override
            public Kernel kernel()
            {
                return kernel;
            }

            @Override
            public Config config()
            {
                return config;
            }
        };
    }
}
