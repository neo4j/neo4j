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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{

    private ExecutorService executor;

    @Override
    public void start()
    {
        this.executor = newCachedThreadPool(new DaemonThreadFactory("Neo4j " + getClass().getSimpleName()));
    }

    @Override
    public void stop()
    {
        this.executor.shutdown();
        this.executor = null;
    }

    @Override
    public void submit( Runnable job )
    {
        this.executor.submit( job );
    }
}
