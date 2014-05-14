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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

/**
 * {@link ExecutionMonitor} that polls the {@link StageExecution} about up to date stats.
 * An {@code interval} can be supplied (millis), where {@link #poll(StageExecution)} will be called,
 * a method that is up to subclasses to implement.
 */
public abstract class PollingExecutionMonitor implements ExecutionMonitor
{
    private final long interval;

    public PollingExecutionMonitor( long interval )
    {
        this.interval = interval;
    }

    @Override
    public void monitor( StageExecution execution )
    {
        long startTime = currentTimeMillis();
        start( execution );
        while ( execution.stillExecuting() )
        {
            poll( execution );
            finishAwareSleep( execution );
        }
        end( execution, currentTimeMillis()-startTime );
    }

    protected void end( StageExecution execution, long totalTimeMillis )
    {   // Nothing by default
    }

    protected void start( StageExecution execution )
    {   // Nothing by default
    }

    protected abstract void poll( StageExecution execution );

    private void finishAwareSleep( StageExecution execution )
    {
        long endTime = currentTimeMillis()+interval;
        while ( currentTimeMillis() < endTime )
        {
            if ( !execution.stillExecuting() )
            {
                break;
            }

            try
            {
                sleep( min( 10, max( 0, endTime-currentTimeMillis() ) ) );
            }
            catch ( InterruptedException e )
            {
                execution.panic( e );
                break;
            }
        }
    }
}
