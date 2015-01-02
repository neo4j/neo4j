/**
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
package org.neo4j.bench;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class BenchmarkResults
{
    private final PrintStream out;
    private final String elapsedTimeUnit;
    private final String durationUnit;

    public BenchmarkResults( File outputResultsFile, String elapsedTimeUnit, String durationUnit ) throws FileNotFoundException
    {
        this.out = new PrintStream( outputResultsFile );
        this.elapsedTimeUnit = elapsedTimeUnit;
        this.durationUnit = durationUnit;
        writeHeader();
    }

    private void writeHeader()
    {
        out.printf( "ElapsedTime_%s\tOperation\tSuccesses\tFailures\tDuration_%s%n", elapsedTimeUnit, durationUnit );
    }

    public void writeResult( long elapsedTime, String operation, int successes, int failures, long duration )
    {
        out.printf("%d\t%s\t%d\t%d\t%d%n", elapsedTime, operation, successes, failures, duration );
    }

    public void close()
    {
        out.close();
    }
}
