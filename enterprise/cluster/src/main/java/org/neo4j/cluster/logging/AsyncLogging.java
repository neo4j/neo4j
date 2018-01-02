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
package org.neo4j.cluster.logging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.concurrent.AsyncEvents;
import org.neo4j.function.Consumer;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.async.AsyncLogEvent;
import org.neo4j.logging.async.AsyncLogProvider;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

public class AsyncLogging extends LifecycleAdapter implements Consumer<AsyncLogEvent>, AsyncEvents.Monitor
{
    private static final boolean ENABLED = flag( AsyncLogging.class, "ENABLED", true );

    public static LogProvider provider( LifeSupport life, LogProvider provider )
    {
        if ( ENABLED )
        {
            if ( provider instanceof NullLogProvider )
            {
                return provider;
            }
            return new AsyncLogProvider( life.add(
                    new AsyncLogging( provider.getLog( AsyncLogging.class ) ) ).eventSender(), provider );
        }
        else
        {
            return provider;
        }
    }

    private final Log metaLog;
    private final AsyncEvents<AsyncLogEvent> events;
    private long highCount;
    private ExecutorService executor;

    AsyncLogging( Log metaLog )
    {
        this.metaLog = metaLog;
        this.events = new AsyncEvents<>( this, this );
    }

    @Override
    public void accept( AsyncLogEvent event )
    {
        event.process();
    }

    @Override
    public void start()
    {
        highCount = 0;
        executor = Executors.newSingleThreadExecutor( new NamedThreadFactory( getClass().getSimpleName() ) );
        executor.submit( events );
        events.awaitStartup();
    }

    @Override
    public void stop() throws InterruptedException
    {
        events.shutdown();
        executor.shutdown();
        events.awaitTermination();
    }

    @Override
    public void eventCount( long count )
    {
        if ( metaLog.isDebugEnabled() )
        {
            if ( count > highCount )
            {
                metaLog.debug( "High mark increasing from %d to %d events", highCount, count );
                highCount = count;
            }
        }
    }

    AsyncEventSender<AsyncLogEvent> eventSender()
    {
        return events;
    }
}
