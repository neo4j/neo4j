/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Acts as a holder of multiple {@link Lifecycle} and executes each transition,
 * all the individual lifecycles in parallel.
 * <p>
 * This is only a test utility and so doesn't support
 */
class ParallelLifecycle extends LifecycleAdapter
{
    private final List<Lifecycle> lifecycles = new ArrayList<>();
    private final long timeout;
    private final TimeUnit unit;

    public ParallelLifecycle( long timeout, TimeUnit unit )
    {
        this.timeout = timeout;
        this.unit = unit;
    }

    public <T extends Lifecycle> T add( T lifecycle )
    {
        lifecycles.add( lifecycle );
        return lifecycle;
    }

    @Override
    public void init() throws Throwable
    {
        perform( lifecycle -> lifecycle.init() );
    }

    @Override
    public void start() throws Throwable
    {
        perform( lifecycle -> lifecycle.start() );
    }

    @Override
    public void stop() throws Throwable
    {
        perform( lifecycle -> lifecycle.stop() );
    }

    @Override
    public void shutdown() throws Throwable
    {
        perform( lifecycle -> lifecycle.shutdown() );
    }

    private void perform( Action action ) throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( lifecycles.size() );
        for ( Lifecycle lifecycle : lifecycles )
        {
            service.submit( () ->
            {
                try
                {
                    action.act( lifecycle );
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
            } );
        }

        service.shutdown();
        if ( !service.awaitTermination( timeout, unit ) )
        {
            throw new IllegalStateException( "Couldn't perform all actions" );
        }
    }

    private interface Action
    {
        void act( Lifecycle lifecycle ) throws Throwable;
    }
}
