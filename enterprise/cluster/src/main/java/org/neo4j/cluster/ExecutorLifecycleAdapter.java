/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ExecutorLifecycleAdapter
    implements Lifecycle, Executor
{
    private ExecutorService executor;

    private Factory<ExecutorService> executorServiceFactory;

    public ExecutorLifecycleAdapter( Factory<ExecutorService> executorServiceFactory )
    {
        this.executorServiceFactory = executorServiceFactory;

    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        executor = executorServiceFactory.newInstance();
    }

    @Override
    public void stop() throws Throwable
    {
        if ( executor != null )
        {
            executor.shutdown();
            executor.awaitTermination( 30, TimeUnit.SECONDS );
            executor = null;
        }
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public void execute( Runnable command )
    {
        if ( executor != null )
        {
            executor.execute( command );
        }
        else
        {
            command.run();
        }
    }
}
