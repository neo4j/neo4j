/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.kernel.InternalAbstractGraphDatabase.Configuration.statistics_enabled;

public class StatisticsServiceRepository
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final StoreReadLayer store;
    private final JobScheduler scheduler;

    public StatisticsServiceRepository( FileSystemAbstraction fs, Config config, StoreReadLayer store,
                                        JobScheduler scheduler )
    {
        this.fs = fs;
        this.config = config;
        this.store = store;
        this.scheduler = scheduler;
    }

    public StatisticsService loadStatistics()
    {
        SamplingStatisticsService runtime = SamplingStatisticsService.load( this.fs, statisticsFile(), store,
                scheduler );
        if(config.get( statistics_enabled ))
        {
            return runtime;
        }
        else
        {
            return new StaleStatisticsService( runtime );
        }
    }

    public void storeStatistics( StatisticsService statistics ) throws IOException
    {
        if(statistics instanceof SamplingStatisticsService )
        {
            ((SamplingStatisticsService)statistics).save( fs, statisticsFile() );
        }
    }

    private File statisticsFile()
    {
        return new File( this.config.get( store_dir ), "neo4j.statistics");
    }

}
