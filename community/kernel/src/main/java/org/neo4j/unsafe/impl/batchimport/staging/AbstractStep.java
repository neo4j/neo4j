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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.Exceptions;
import org.neo4j.unsafe.impl.batchimport.stats.ProcessingStats;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.System.currentTimeMillis;

/**
 * Basic implementation of a {@link Step}. Does the most plumbing job of building a step implementation.
 */
public abstract class AbstractStep<T> implements Step<T>
{
    private final StageControl control;
    private final String name;
    @SuppressWarnings( "rawtypes" )
    private volatile Step downstream;
    protected volatile boolean endOfUpstream;
    protected final AtomicLong downstreamIdleTime = new AtomicLong();
    protected final AtomicLong upstreamIdleTime = new AtomicLong();
    protected final AtomicLong receivedBatches = new AtomicLong();
    protected final AtomicLong doneBatches = new AtomicLong();
    protected final AtomicLong totalProcessingTime = new AtomicLong();
    private volatile boolean panic;
    private volatile boolean completed;

    public AbstractStep( StageControl control, String name )
    {
        this.control = control;
        this.name = name;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        this.panic = true;
    }

    protected boolean stillWorking()
    {
        // If there's a panic we'll just stop working
        if ( panic )
        {
            return false;
        }

        // If upstream has run out and we've processed all
        if ( endOfUpstream && doneBatches.get() == receivedBatches.get() )
        {
            return false;
        }

        // We're still working
        return true;
    }

    @Override
    public boolean isCompleted()
    {
        return completed;
    }

    protected void issuePanic( Throwable cause )
    {
        control.panic( cause );
        throw Exceptions.launderedException( cause );
    }

    protected long awaitTicket( long ticket )
    {
        long waitTime = currentTimeMillis();
        while ( receivedBatches.get() != ticket-1 )
        {
            waitSome();
        }
        return currentTimeMillis()-waitTime;
    }

    protected void assertHealthy()
    {
        if ( panic )
        {
            throw new RuntimeException( "Panic called, so exiting" );
        }
    }

    protected void waitSome()
    {
        try
        {
            Thread.sleep( 1 );
            Thread.yield();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void ticketReceived( long ticket )
    {
        if ( !receivedBatches.compareAndSet( ticket-1, ticket ) )
        {
            throw new IllegalStateException();
        }
    }

    @Override
    public void setDownstream( Step<?> downstream )
    {
        this.downstream = downstream;
    }

    @Override
    public StepStats stats()
    {
        Collection<StatsProvider> providers = new ArrayList<>();
        addStatsProviders( providers );
        return new StepStats( name, stillWorking(), providers );
    }

    protected void addStatsProviders( Collection<StatsProvider> providers )
    {
        providers.add( new ProcessingStats( receivedBatches.get(), doneBatches.get(), totalProcessingTime.get(),
                upstreamIdleTime.get(), downstreamIdleTime.get() ) );
    }

    @SuppressWarnings( "unchecked" )
    protected <BATCH> void sendDownstream( long ticket, BATCH batch )
    {
        if ( batch == null )
        {
            if ( downstream != null )
            {
                throw new IllegalArgumentException( "Expected a batch to send downstream" );
            }
        }
        else
        {
            downstreamIdleTime.addAndGet( downstream.receive( ticket, batch ) );
        }
        checkNotifyEndDownstream();
    }

    @Override
    public void endOfUpstream()
    {
        endOfUpstream = true;
        checkNotifyEndDownstream();
    }

    protected void checkNotifyEndDownstream()
    {
        if ( !stillWorking() && !isCompleted() )
        {
            done();
            if ( downstream != null )
            {
                downstream.endOfUpstream();
            }
            // else this is the end of the line
            completed = true;
        }
    }

    protected void done()
    {   // Do nothing by default
    }
}
