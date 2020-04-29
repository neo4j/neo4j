/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.batchimport.staging;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.batchimport.stats.DetailLevel;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.batchimport.stats.StepStats;
import org.neo4j.internal.helpers.collection.Pair;

import static java.lang.Math.pow;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.internal.helpers.collection.Iterables.last;

/**
 * This is supposed to be a beautiful one-line {@link ExecutionMonitor}, looking like:
 *
 * <pre>
 * NODE |--INPUT--|--NODE--|======NODE=PROPERTY======|-------------WRITER-------------| 1000
 * </pre>
 *
 * where there's one line per stage, updated rapidly, overwriting the line each time. The width
 * of the {@link Step} column is based on how slow it is compared to the others.
 *
 * The width of the "spectrum" is user specified, but is dynamic in that it can shrink or expand
 * based on how many simultaneous {@link StageExecution executions} this monitor is monitoring.
 *
 * The specified width is included stage identifier and progress, so in a console the whole
 * console width can be specified.
 */
public class SpectrumExecutionMonitor extends ExecutionMonitor.Adapter
{
    public static final int DEFAULT_WIDTH = 100;
    private static final int PROGRESS_WIDTH = 5;
    private static final char[] WEIGHTS = new char[] {' ', 'K', 'M', 'B', 'T'};

    private final PrintStream out;
    private final int width;
    // For tracking delta
    private long lastProgress;

    public SpectrumExecutionMonitor( long interval, TimeUnit unit, PrintStream out, int width )
    {
        super( interval, unit );
        this.out = out;
        this.width = width;
    }

    @Override
    public void start( StageExecution execution )
    {
        out.println( execution.name() + ", started " + date() );
        lastProgress = 0;
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        check( execution );
        out.println();
        out.println( "Done in " + duration( totalTimeMillis ) );
    }

    @Override
    public void done( boolean successful, long totalTimeMillis, String additionalInformation )
    {
        out.println();
        out.println( format( "IMPORT %s in %s. %s", successful ? "DONE" : "FAILED", duration( totalTimeMillis ), additionalInformation ) );
    }

    @Override
    public void check( StageExecution execution )
    {
        StringBuilder builder = new StringBuilder();
        printSpectrum( builder, execution, width, DetailLevel.IMPORTANT );

        // add delta
        long progress = last( execution.steps() ).stats().stat( Keys.done_batches ).asLong() * execution.getConfig().batchSize();
        long currentDelta = progress - lastProgress;
        builder.append( " ∆" ).append( fitInProgress( currentDelta ) );

        // and remember progress to compare with next check
        lastProgress = progress;

        // print it (overwriting the previous contents on this console line)
        out.print( "\r" + builder );
    }

    public static void printSpectrum( StringBuilder builder, StageExecution execution, int width, DetailLevel additionalStatsLevel )
    {
        long[] values = values( execution );
        long total = total( values );

        // reduce the width with the known extra characters we know we'll print in and around the spectrum
        width -= 2/*'[]' chars*/ + PROGRESS_WIDTH/*progress chars*/;

        Pair<Step<?>,Float> bottleNeck = execution.stepsOrderedBy( Keys.avg_processing_time, false ).iterator().next();
        QuantizedProjection projection = new QuantizedProjection( total, width );
        long lastDoneBatches = 0;
        int stepIndex = 0;
        boolean hasProgressed = false;
        builder.append( '[' );
        for ( Step<?> step : execution.steps() )
        {
            StepStats stats = step.stats();
            if ( !projection.next( values[stepIndex] ) )
            {
                break; // odd though
            }
            long stepWidth = total == 0 && stepIndex == 0 ? width : projection.step();
            if ( stepWidth > 0 )
            {
                if ( hasProgressed )
                {
                    stepWidth--;
                    builder.append( '|' );
                }
                boolean isBottleNeck = bottleNeck.first() == step;
                String name =
                        (isBottleNeck ? "*" : "") +
                        stats.toString( additionalStatsLevel ) + (step.processors( 0 ) > 1
                        ? "(" + step.processors( 0 ) + ")"
                        : "");
                int charIndex = 0; // negative value "delays" the text, i.e. pushes it to the right
                char backgroundChar = step.processors( 0 ) > 1 ? '=' : '-';
                for ( int i = 0; i < stepWidth; i++, charIndex++ )
                {
                    char ch = backgroundChar;
                    if ( charIndex >= 0 && charIndex < name.length() && charIndex < stepWidth )
                    {
                        ch = name.charAt( charIndex );
                    }
                    builder.append( ch );
                }
                hasProgressed = true;
            }
            lastDoneBatches = stats.stat( Keys.done_batches ).asLong();
            stepIndex++;
        }

        long progress = lastDoneBatches * execution.getConfig().batchSize();
        builder.append( ']' ).append( fitInProgress( progress ) );
    }

    private static String fitInProgress( long value )
    {
        int weight = weight( value );

        String progress;
        if ( weight == 0 )
        {
            progress = String.valueOf( value );
        }
        else
        {
            double floatValue = value / pow( 1000, weight );
            progress = String.valueOf( floatValue );
            if ( progress.length() > PROGRESS_WIDTH - 1 )
            {
                progress = progress.substring( 0, PROGRESS_WIDTH - 1 );
            }
            if ( progress.endsWith( "." ) )
            {
                progress = progress.substring( 0, progress.length() - 1 );
            }
            progress += WEIGHTS[weight];
        }

        return pad( progress, PROGRESS_WIDTH, ' ' );
    }

    private static String pad( String result, int length, char padChar )
    {
        while ( result.length() < length )
        {
            result = padChar + result;
        }
        return result;
    }

    private static int weight( long value )
    {
        int weight = 0;
        while ( value >= 1000 )
        {
            value /= 1000;
            weight++;
        }
        return weight;
    }

    private static long[] values( StageExecution execution )
    {
        long[] values = new long[execution.size()];
        int i = 0;
        for ( Step<?> step : execution.steps() )
        {
            values[i++] = avg( step.stats() );
        }
        return values;
    }

    private static long total( long[] values )
    {
        long total = 0;
        for ( long value : values )
        {
            total += value;
        }
        return total;
    }

    private static long avg( StatsProvider step )
    {
        return step.stat( Keys.avg_processing_time ).asLong();
    }
}
