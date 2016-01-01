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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.heuristics.StatisticsData;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;


public class SamplingStatisticsService extends LifecycleAdapter implements StatisticsService
{
    private final JobScheduler scheduler;
    private final StatisticsCollector collector;

    public static SamplingStatisticsService load( FileSystemAbstraction fs, File path, StoreReadLayer store,
                                                 JobScheduler scheduler )
    {
        if ( fs.fileExists( path ) )
        {
            try
            {
                ObjectInputStream in = new ObjectInputStream( fs.openAsInputStream( path ) );
                return new SamplingStatisticsService( (StatisticsCollectedData) in.readObject(), store, scheduler );
            }
            catch ( Exception e )
            {
                // Ignore. This would indicate the file is somehow corrupt, so just start over with new statistics.
            }
        }

        return new SamplingStatisticsService( store, scheduler );
    }

    public SamplingStatisticsService( StoreReadLayer store, JobScheduler scheduler )
    {
        this( new StatisticsCollectedData(), store, scheduler );
    }

    public SamplingStatisticsService( StatisticsCollectedData collectedData, StoreReadLayer store,
                                      JobScheduler scheduler )
    {
        this.scheduler = scheduler;
        this.collector = new StatisticsCollector( store, collectedData );
    }

    @Override
    public void start() throws Throwable
    {
        scheduler.scheduleRecurring( JobScheduler.Group.heuristics, collector, 30, TimeUnit.SECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        scheduler.cancelRecurring( JobScheduler.Group.heuristics, collector );
    }

    @Override
    public StatisticsData statistics()
    {
        return collector.collectedData();
    }

    public void save( FileSystemAbstraction fs, File path ) throws IOException
    {
        fs.create( path );
        try ( OutputStream out = fs.openAsOutputStream( path, false ) )
        {
            ObjectOutputStream objStream = new ObjectOutputStream( out );
            objStream.writeObject( this.collector.collectedData() );
            objStream.close();
            out.flush();
        }
    }
}
