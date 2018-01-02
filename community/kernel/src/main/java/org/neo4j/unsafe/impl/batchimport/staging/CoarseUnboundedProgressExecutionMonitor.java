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

import java.io.PrintStream;

import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.helpers.collection.IteratorUtil.last;

/**
 * {@link ExecutionMonitor} that prints progress, e.g. a dot every N batches completed.
 */
public class CoarseUnboundedProgressExecutionMonitor extends ExecutionMonitor.Adapter
{
    private int prevN = 0;
    private final int dotEveryN;
    private final PrintStream out;

    public CoarseUnboundedProgressExecutionMonitor( int dotEveryN, PrintStream out )
    {
        super( 100, MILLISECONDS );
        this.dotEveryN = dotEveryN;
        this.out = out;
    }

    @Override
    public void start( StageExecution[] executions )
    {
        prevN = 0;
    }

    @Override
    public void check( StageExecution[] executions )
    {
        int n = prevN;
        for ( StageExecution execution : executions )
        {
            n = max( n, n( execution ) );
        }

        while ( prevN < n )
        {
            progress();
            prevN++;
        }
    }

    private int n( StageExecution execution )
    {
        long doneBatches = last( execution.steps() ).stats().stat( Keys.done_batches ).asLong();
        int batchSize = execution.getConfig().batchSize();
        long amount = doneBatches*batchSize;
        int n = (int) (amount/dotEveryN);
        return n;
    }

    protected void progress()
    {
        out.print( "." );
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        out.println();
    }
}
