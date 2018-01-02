/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Default implementation of {@link StageControl}
 */
public class StageExecution implements StageControl
{
    private final String stageName;
    private final Configuration config;
    private final Collection<Step<?>> pipeline;
    private volatile Throwable panicCause;
    private long startTime;
    private final int orderingGuarantees;

    public StageExecution( String stageName, Configuration config, Collection<Step<?>> pipeline, int orderingGuarantees )
    {
        this.stageName = stageName;
        this.config = config;
        this.pipeline = pipeline;
        this.orderingGuarantees = orderingGuarantees;
    }

    public boolean stillExecuting()
    {
        Throwable panic = panicCause;
        if ( panic != null )
        {
            String message = panic.getMessage();
            throw launderedException( message == null? "Panic" : message, panic );
        }

        for ( Step<?> step : pipeline )
        {
            if ( !step.isCompleted() )
            {
                return true;
            }
        }
        return false;
    }

    public void start()
    {
        this.startTime = currentTimeMillis();
        for ( Step<?> step : pipeline )
        {
            step.start( orderingGuarantees );
        }
    }

    public long getExecutionTime()
    {
        return currentTimeMillis()-startTime;
    }

    public String getStageName()
    {
        return stageName;
    }

    public Configuration getConfig()
    {
        return config;
    }

    public Iterable<Step<?>> steps()
    {
        return pipeline;
    }

    /**
     * @param stat statistics {@link Key}.
     * @param trueForAscending {@code true} for ordering by ascending, otherwise descending.
     * @return the steps ordered by the {@link Stat#asLong() long value representation} of the given
     * {@code stat} accompanied a factor by how it compares to the next value, where a value close to
     * {@code 1.0} signals them being close to equal, and a value of for example {@code 0.5} signals that
     * the value of the current step is half that of the next step.
     */
    public Iterable<Pair<Step<?>,Float>> stepsOrderedBy( final Key stat, final boolean trueForAscending )
    {
        final List<Step<?>> steps = new ArrayList<>( pipeline );
        Collections.sort( steps, new Comparator<Step<?>>()
        {
            @Override
            public int compare( Step<?> o1, Step<?> o2 )
            {
                Long stat1 = o1.stats().stat( stat ).asLong();
                Long stat2 = o2.stats().stat( stat ).asLong();
                return trueForAscending
                        ? stat1.compareTo( stat2 )
                        : stat2.compareTo( stat1 );
            }
        } );

        return new Iterable<Pair<Step<?>,Float>>()
        {
            @Override
            public Iterator<Pair<Step<?>,Float>> iterator()
            {
                return new PrefetchingIterator<Pair<Step<?>,Float>>()
                {
                    private final Iterator<Step<?>> source = steps.iterator();
                    private Step<?> next = source.hasNext() ? source.next() : null;

                    @Override
                    protected Pair<Step<?>,Float> fetchNextOrNull()
                    {
                        if ( next == null )
                        {
                            return null;
                        }

                        Step<?> current = next;
                        next = source.hasNext() ? source.next() : null;
                        float factor = next != null
                                ? (float) stat( current, stat ) / (float) stat( next, stat )
                                : 1.0f;
                        return Pair.<Step<?>, Float> of( current, factor );
                    }

                    private long stat( Step<?> step, Key stat )
                    {
                        return step.stats().stat( stat ).asLong();
                    }
                };
            }
        };
    }

    public int size()
    {
        return pipeline.size();
    }

    @Override
    public void panic( Throwable cause )
    {
        panicCause = cause;
        for ( Step<?> step : pipeline )
        {
            step.receivePanic( cause );
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + stageName + "]";
    }
}
