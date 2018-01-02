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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Factory;
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
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        executor = executorServiceFactory.newInstance();
    }

    @Override
    public void stop() throws Throwable
    {
        if (executor != null)
        {
            executor.shutdown();
            executor.awaitTermination( 30, TimeUnit.SECONDS );
            executor = null;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public void execute( Runnable command )
    {
        if (executor != null)
            executor.execute( command );
        else
        {
            command.run();
        }
    }
}
