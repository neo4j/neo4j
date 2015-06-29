/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.stresstests;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.stresstests.workload.Workload;

import static org.junit.Assert.assertTrue;

public class TransactionThroughputChecker implements Workload.TransactionThroughput
{
    private final List<Double> reports = new ArrayList<>();

    @Override
    public void report( long transactions, long elapsedTime)
    {
        reports.add( ((double) transactions / (double) elapsedTime) );
    }

    public void assertThroughput( PrintStream out )
    {
        // dropping the first report which is


        if ( reports.isEmpty() )
        {
            out.println( "no reports" );
            return;
        }

        out.println( "Throughput reports (tx/ms):" );
        double sum = 0;
        for ( double report : reports )
        {
            out.println( "\t" + report );
            sum += report;
        }
        out.println();

        double average = sum / (double) reports.size();
        out.println( "Average throughput (tx/ms): " + average );
        double tolerance = average * 0.20d;
        out.println( "Start check throughput with tolerance: " + tolerance );

        int nonMatchingCounter = 0;
        for ( double report : reports )
        {
            if ( doubleIsDifferent( average, report, tolerance ) )
            {
                System.err.println( "Found a time slot when throughput differs too much wrt the average:" );
                System.err.println( "\tAverage=" + average + ", current=" + report + ", tolerance=" + tolerance );
                nonMatchingCounter++;
            }
        }
        int nonSuccessPercentage = (int) ((nonMatchingCounter / (double) reports.size()) * 100);
        System.out.println( "Non successful percentage is: " + nonSuccessPercentage );
        assertTrue( "Assumption is that more then 80 percent should be in " +
                    "range [average - (average * 0.2 ); average  + (average * 0.2)] ",
                nonSuccessPercentage < 20 );
    }

    private boolean doubleIsDifferent( double expected, double actual, double tolerance )
    {
        return Double.compare( expected, actual ) != 0 && Math.abs( expected - actual ) > tolerance;
    }
}
