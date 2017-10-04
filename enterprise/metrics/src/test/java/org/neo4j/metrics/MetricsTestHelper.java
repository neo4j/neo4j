/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class MetricsTestHelper
{
    private static final int TIME_STAMP = 0;
    private static final int METRICS_VALUE = 1;

    private MetricsTestHelper()
    {
    }

    public static long readLongValue( File metricFile ) throws IOException, InterruptedException
    {
        return readLongValueAndAssert( metricFile, ( one, two ) -> true );
    }

    static long readLongValueAndAssert( File metricFile, BiPredicate<Long,Long> assumption )
            throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0L, Long::parseLong, assumption );
    }

    static double readDoubleValue( File metricFile ) throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0.0, Double::parseDouble,
                ( one, two ) -> true );
    }

    private static <T> T readValueAndAssert( File metricFile, T startValue, Function<String,T> parser,
            BiPredicate<T,T> assumption ) throws IOException, InterruptedException
    {
        // let's wait until the file is in place (since the reporting is async that might take a while)
        assertEventually( "Metrics file should exist", metricFile::exists, is( true ), 40, SECONDS );

        try ( BufferedReader reader = new BufferedReader( new FileReader( metricFile ) ) )
        {
            String s;
            do
            {
                s = reader.readLine();
            }
            while ( s == null );
            String[] headers = s.split( "," );
            assertThat( headers.length, is( 2 ) );
            assertThat( headers[TIME_STAMP], is( "t" ) );
            assertThat( headers[METRICS_VALUE], is( "value" ) );

            T currentValue = startValue;
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "," );
                T newValue = parser.apply( fields[1] );
                assertTrue( "assertion failed on " + newValue + " " + currentValue,
                        assumption.test( newValue, currentValue ) );
                currentValue = newValue;
            }
            return currentValue;
        }
    }

    public static File metricsCsv( File dbDir, String metric ) throws InterruptedException
    {
        File csvFile = new File( dbDir, metric + ".csv" );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 40, SECONDS );
        return csvFile;
    }
}
