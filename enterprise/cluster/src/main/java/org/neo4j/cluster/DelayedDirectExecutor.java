/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Executor that executes the Runnables when drain() is called. Allows async jobs to be scheduled, and then
 * run in a synchronous fashion.
 */
public class DelayedDirectExecutor extends AbstractExecutorService
{
    private List<Runnable> runnables = new ArrayList<Runnable>();

    private final Log log;

    public DelayedDirectExecutor( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination( long timeout, TimeUnit unit ) throws InterruptedException
    {
        return true;
    }

    @Override
    public synchronized void execute( Runnable command )
    {
        runnables.add( command );
    }

    public void drain()
    {
        List<Runnable> currentRunnables;
        synchronized ( this )
        {
            currentRunnables = runnables;
            runnables = new ArrayList<Runnable>();
        }
        for ( Runnable runnable : currentRunnables )
        {
            try
            {
                runnable.run();
            }
            catch ( Throwable t )
            {
                log.error( "Runnable failed", t );
            }
        }
    }
}
