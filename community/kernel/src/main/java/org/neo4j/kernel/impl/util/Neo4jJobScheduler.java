/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private final StringLogger log;
    private final String id;

    private ExecutorService executor;
    private Timer timer; // Note, we may want a pool of these in the future, to minimize contention.

    public Neo4jJobScheduler( StringLogger log )
    {
        this.log = log;
        this.id = getClass().getSimpleName();
    }

    public Neo4jJobScheduler( String id, StringLogger log )
    {
        this.log = log;
        this.id = id;
    }

    @Override
    public void start()
    {
        this.executor = newCachedThreadPool(new DaemonThreadFactory("Neo4j " + id));
        this.timer = new Timer( "Neo4j Recurring Job Runner", /* daemon= */true );
    }

    @Override
    public void schedule( Runnable job )
    {
        this.executor.submit( job );
    }

    @Override
    public void scheduleRecurring( final Runnable runnable, long period, TimeUnit timeUnit )
    {
        timer.schedule( new TimerTask()
        {
            @Override
            public void run()
            {
                try
                {
                    runnable.run();
                } catch(RuntimeException e)
                {
                    log.error( "Failed running recurring job.", e );
                }
            }
        }, 0, timeUnit.toMillis( period ) );
    }

    @Override
    public void stop()
    {
        RuntimeException exception = null;
        try
        {
            if(executor != null)
            {
                executor.shutdown();
                executor = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }
        try
        {
            if(timer != null)
            {
                timer.cancel();
                timer = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }

        if(exception != null)
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception);
        }
    }
}
