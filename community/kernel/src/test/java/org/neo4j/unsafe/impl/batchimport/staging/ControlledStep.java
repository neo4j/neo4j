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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static org.junit.Assert.assertTrue;

/**
 * A bit like a mocked {@link Step}, but easier to work with.
 */
public class ControlledStep<T> implements Step<T>, StatsProvider
{
    public static ControlledStep<?> stepWithAverageOf( String name, int maxProcessors, long avg )
    {
        return stepWithStats( name, maxProcessors, Keys.avg_processing_time, avg );
    }

    public static ControlledStep<?> stepWithStats( String name, int maxProcessors,
            Map<Key,Long> statistics )
    {
        ControlledStep<?> step = new ControlledStep<>( name, maxProcessors );
        for ( Map.Entry<Key,Long> statistic : statistics.entrySet() )
        {
            step.setStat( statistic.getKey(), statistic.getValue().longValue() );
        }
        return step;
    }

    public static ControlledStep<?> stepWithStats( String name, int maxProcessors, Object... statisticsAltKeyAndValue )
    {
        return stepWithStats( name, maxProcessors, MapUtil.<Key,Long>genericMap( statisticsAltKeyAndValue ) );
    }

    private final String name;
    private final Map<Key,ControlledStat> stats = new HashMap<>();
    private final int maxProcessors;
    private volatile int numberOfProcessors = 1;

    public ControlledStep( String name, int maxProcessors )
    {
        this( name, maxProcessors, 1 );
    }

    public ControlledStep( String name, int maxProcessors, int initialProcessorCount )
    {
        this.maxProcessors = maxProcessors == 0 ? Integer.MAX_VALUE : maxProcessors;
        this.name = name;
        setNumberOfProcessors( initialProcessorCount );
    }

    @Override
    public int numberOfProcessors()
    {
        return numberOfProcessors;
    }

    public ControlledStep<T> setNumberOfProcessors( int numberOfProcessors )
    {
        assertTrue( numberOfProcessors <= maxProcessors );
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    @Override
    public synchronized boolean incrementNumberOfProcessors()
    {
        if ( numberOfProcessors >= maxProcessors )
        {
            return false;
        }
        numberOfProcessors++;
        return true;
    }

    @Override
    public synchronized boolean decrementNumberOfProcessors()
    {
        if ( numberOfProcessors == 1 )
        {
            return false;
        }
        numberOfProcessors--;
        return true;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public long receive( long ticket, T batch )
    {
        throw new UnsupportedOperationException( "Cannot participate in actual processing yet" );
    }

    public void setStat( Key key, long value )
    {
        stats.put( key, new ControlledStat( value ) );
    }

    @Override
    public StepStats stats()
    {
        return new StepStats( name, !isCompleted(), Arrays.<StatsProvider>asList( this ) );
    }

    @Override
    public void endOfUpstream()
    {
    }

    @Override
    public boolean isCompleted()
    {
        return false;
    }

    @Override
    public void setDownstream( Step<?> downstreamStep )
    {
    }

    @Override
    public void receivePanic( Throwable cause )
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public Stat stat( Key key )
    {
        return stats.get( key );
    }

    @Override
    public void start( int orderingGuarantees )
    {
    }

    @Override
    public Key[] keys()
    {
        return stats.keySet().toArray( new Key[stats.size()] );
    }

    private static class ControlledStat implements Stat
    {
        private final long value;

        ControlledStat( long value )
        {
            this.value = value;
        }

        @Override
        public DetailLevel detailLevel()
        {
            return DetailLevel.BASIC;
        }

        @Override
        public long asLong()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "" + value;
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + name() + ", " + stats + "]";
    }
}
