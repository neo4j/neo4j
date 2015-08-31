/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

        double powerSum = 0.0;
        for ( double report : reports )
        {
            powerSum += Math.pow(report - average, 2);
        }

        double stdDeviation = Math.sqrt( powerSum / (double) reports.size() );
        out.println( "Standard deviation (tx/ms): " + stdDeviation );
        double twoStdDeviations = stdDeviation * 2.0;
        out.println( "Two standard deviations (tx/ms): " + twoStdDeviations );

        int inOneStdDeviationRange = 0;
        int inTwoStdDeviationRange = 0;
        for ( double report : reports )
        {
            if ( Math.abs( average - report ) <= stdDeviation )
            {
                inOneStdDeviationRange++;
                inTwoStdDeviationRange++;
            }
            else if ( Math.abs( average - report ) <= twoStdDeviations )
            {
                System.err.println( "Outside _one_ std deviation range: " + report );
                inTwoStdDeviationRange++;
            }
            else
            {
                System.err.println( "Outside _two_ std deviation range: " + report );
            }
        }

        int inOneStdDeviationRangePercentage =
                (int) ( (inOneStdDeviationRange  * 100.0) / (double) reports.size() );
        System.out.println( "Percentage inside one std deviation is: " + inOneStdDeviationRangePercentage );
        assertTrue( "Assumption is that at least 65 percent should be in one std deviation (" + stdDeviation  + ")" +
                    " range from the average (" + average + ") ", inOneStdDeviationRangePercentage >= 65 );

        int inTwoStdDeviationRangePercentage =
                (int) ( (inTwoStdDeviationRange  * 100.0) / (double) reports.size() );
        System.out.println( "Percentage inside two std deviations is: " + inTwoStdDeviationRangePercentage );
        assertTrue( "Assumption is that at least 90 percent should be in two std deviations (" + twoStdDeviations + ")" +
                    " range from the average (" + average + ") ", inTwoStdDeviationRangePercentage >= 90 );
    }

}
