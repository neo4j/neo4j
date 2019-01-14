/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    ParallelLifecycle( long timeout, TimeUnit unit )
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
        perform( Lifecycle::init );
    }

    @Override
    public void start() throws Throwable
    {
        perform( Lifecycle::start );
    }

    @Override
    public void stop() throws Throwable
    {
        perform( Lifecycle::stop );
    }

    @Override
    public void shutdown() throws Throwable
    {
        perform( Lifecycle::shutdown );
    }

    private void perform( Action action ) throws Exception
    {
        ExecutorService service = Executors.newFixedThreadPool( lifecycles.size() );
        List<Future<?>> futures = new ArrayList<>();
        for ( Lifecycle lifecycle : lifecycles )
        {
            futures.add( service.submit( () ->
            {
                try
                {
                    action.act( lifecycle );
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
            } ) );
        }

        service.shutdown();
        if ( !service.awaitTermination( timeout, unit ) )
        {
            for ( Future<?> future : futures )
            {
                future.cancel( true );
            }
        }

        Exception exception = null;
        for ( Future<?> future : futures )
        {
            try
            {
                future.get();
            }
            catch ( InterruptedException | ExecutionException e )
            {
                if ( exception == null )
                {
                    exception = new RuntimeException();
                }
                exception.addSuppressed( e );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    private interface Action
    {
        void act( Lifecycle lifecycle ) throws Throwable;
    }
}
