/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

public class ListAccumulatorMigrationProgressMonitor implements MigrationProgressMonitor
{
    private final Map<String,AtomicLong> events = new HashMap<>();
    private boolean started = false;
    private boolean finished = false;

    @Override
    public void started()
    {
        started = true;
    }

    @Override
    public Section startSection( String name )
    {
        final AtomicLong progress = new AtomicLong();
        assert events.put( name, progress ) == null;
        return new Section()
        {
            @Override
            public void progress( long add )
            {
                progress.addAndGet( add );
            }

            @Override
            public void start( long max )
            {
            }

            @Override
            public void completed()
            {
            }
        };
    }

    @Override
    public void completed()
    {
        finished = true;
    }

    public Map<String,Long> progresses()
    {
        Map<String,Long> result = new HashMap<>();
        for ( Map.Entry<String,AtomicLong> entry : events.entrySet() )
        {
            result.put( entry.getKey(), entry.getValue().longValue() );
        }
        return result;
    }

    public boolean isStarted()
    {
        return started;
    }

    public boolean isFinished()
    {
        return finished;
    }
}
