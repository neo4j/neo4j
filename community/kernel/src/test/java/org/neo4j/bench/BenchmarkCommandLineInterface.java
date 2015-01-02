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
import java.io.PrintStream;

import static java.lang.System.getProperty;
import static javax.xml.bind.DatatypeConverter.parseInt;

public class BenchmarkCommandLineInterface
{
    public int evaluate( String[] args, Describer describer, RunBenchCase benchCase ) throws Exception
    {
        if ( args.length == 1 )
        {
            if ( "describe".equals( args[0] ) )
            {
                describer.describe( System.out );
                return 0;
            }
            if ( "run".equals( args[0] ) )
            {
                return benchCase.run( new BasicParameters(
                        new File( getProperty( "outputResultsFile" ) ),
                        parseInt( getProperty( "totalDuration" ) ) * 1000 ) );
            }
        }
        System.err.println( "Usage: specify 'run' or 'describe'" );
        return 1;
    }

    public interface RunBenchCase
    {
        int run(BasicParameters parameters) throws Exception;

    }
    public interface Describer
    {
        void describe(PrintStream out);

    }
    public static class BasicParameters
    {
        public final File outputResultsFile;

        public final long totalDuration;
        public BasicParameters( File outputResultsFile, long totalDuration )
        {
            this.outputResultsFile = outputResultsFile;
            this.totalDuration = totalDuration;
        }
    }
}
