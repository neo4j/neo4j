/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.System.currentTimeMillis;

/**
 * Default implementation of {@link StageControl}
 */
public class StageExecution implements StageControl
{
    private final String stageName;
    private final Configuration config;
    private final Iterable<Step<?>> pipeline;
    private final StringLogger logger;
    private volatile Throwable panicCause;
    private long startTime;

    public StageExecution( Logging logging, String stageName, Configuration config, Iterable<Step<?>> pipeline )
    {
        this.stageName = stageName;
        this.config = config;
        this.pipeline = pipeline;
        this.logger = logging.getMessagesLog( getClass() );
    }

    public boolean stillExecuting()
    {
        if ( panicCause != null )
        {
            throw new RuntimeException( "Panic", panicCause );
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

    public Iterable<StepStats> stats()
    {
        return new IterableWrapper<StepStats, Step<?>>( pipeline )
        {
            @Override
            protected StepStats underlyingObjectToObject( Step<?> step )
            {
                return step.stats();
            }
        };
    }

    @Override
    public void panic( Throwable cause )
    {
        cause.printStackTrace();
        logger.error( "Panic", cause );

        panicCause = cause;
        for ( Step<?> step : pipeline )
        {
            step.receivePanic( cause );
        }
    }
}
