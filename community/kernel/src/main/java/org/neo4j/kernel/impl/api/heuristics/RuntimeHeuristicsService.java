/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.heuristics.HeuristicsData;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class RuntimeHeuristicsService extends LifecycleAdapter implements HeuristicsService, Runnable
{
    private final StoreReadLayer store;
    private final JobScheduler scheduler;
    private final Random random = new Random();

    private final HeuristicsCollector collector;

    public static RuntimeHeuristicsService load( FileSystemAbstraction fs, File path, StoreReadLayer store, JobScheduler scheduler )
    {
        if ( fs.fileExists( path ) )
        {
            try
            {
                ObjectInputStream in = new ObjectInputStream( fs.openAsInputStream( path ) );
                return new RuntimeHeuristicsService((HeuristicsCollector)in.readObject(), store, scheduler);
            }
            catch ( Exception e )
            {
                // Ignore. This would indicate the file is somehow corrupt, so just start over with new heuristics.
            }
        }

        return new RuntimeHeuristicsService( store, scheduler );
    }

    public RuntimeHeuristicsService( StoreReadLayer store, JobScheduler scheduler )
    {
        this( new HeuristicsCollector(), store, scheduler );
    }

    public RuntimeHeuristicsService( HeuristicsCollector collector, StoreReadLayer store, JobScheduler scheduler )
    {
        this.store = store;
        this.scheduler = scheduler;
        this.collector = collector;
    }

    @Override
    public void start() throws Throwable
    {
        scheduler.scheduleRecurring(JobScheduler.Group.heuristics, this, 30, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Throwable
    {
        scheduler.cancelRecurring( JobScheduler.Group.heuristics, this );
    }

    /** Perform one sampling run. */
    @Override
    public void run()
    {
        for ( int i = 0; i < 100; i++ ) {
            long id = random.nextLong() % store.highestNodeIdInUse();
            if ( store.nodeExists(id) ) {
                try {
                    List<Integer> relTypes = asList(store.nodeGetRelationshipTypes(id));
                    List<Integer> labels = asList(store.nodeGetLabels(id));

                    Map<Integer, Integer> incomingDegrees = new HashMap<>();
                    Map<Integer, Integer> outgoingDegrees = new HashMap<>();

                    for (Integer relType : relTypes) {
                        incomingDegrees.put(relType, store.nodeGetDegree(id, Direction.INCOMING, relType));
                        outgoingDegrees.put(relType, store.nodeGetDegree(id, Direction.OUTGOING, relType));
                    }

                    collector.addNodeObservation(labels, relTypes, incomingDegrees, outgoingDegrees);
                } catch (EntityNotFoundException e) {
                    // Node was deleted while we read it, or something. In any case, just exclude it from the run.
                    collector.addSkippedNodeObservation();
                }
            }
            else
            {
                collector.addSkippedNodeObservation();
            }
        }

        collector.addMaxNodesObservation(store.highestNodeIdInUse());

        collector.recalculate();
    }

    @Override
    public HeuristicsData heuristics()
    {
        return collector;
    }

    public void save( FileSystemAbstraction fs, File path ) throws IOException
    {
        fs.create( path );
        try ( OutputStream out = fs.openAsOutputStream( path, false ) )
        {
            ObjectOutputStream objStream = new ObjectOutputStream( out );
            objStream.writeObject( this.collector );
            objStream.close();
            out.flush();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        RuntimeHeuristicsService that = (RuntimeHeuristicsService) o;
        return collector.equals( that.collector );

    }

    @Override
    public int hashCode()
    {
        return collector.hashCode();
    }
}
