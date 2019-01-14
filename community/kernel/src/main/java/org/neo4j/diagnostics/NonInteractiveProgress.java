/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.diagnostics;

import java.io.PrintStream;

public class NonInteractiveProgress implements DiagnosticsReporterProgress
{
    private String totalSteps = "?";
    private final PrintStream out;
    private final boolean verbose;
    private int lastPercentage;

    public NonInteractiveProgress( PrintStream out, boolean verbose )
    {
        this.out = out;
        this.verbose = verbose;
    }

    @Override
    public void percentChanged( int percent )
    {
        for ( int i = lastPercentage + 1; i <= percent; i++ )
        {
            out.print( '.' );
            if ( i % 20 == 0 )
            {
                out.printf( " %3d%%%n", i );
            }
        }
        lastPercentage = percent;
        out.flush();
    }

    @Override
    public void started( long currentStepIndex, String target )
    {
        out.println( currentStepIndex + "/" + totalSteps + " " + target );
        lastPercentage = 0;
    }

    @Override
    public void finished()
    {
        percentChanged( 100 );
        out.println();
    }

    @Override
    public void info( String info )
    {
        // Ignore info message
    }

    @Override
    public void error( String msg, Throwable throwable )
    {
        out.println();
        out.println( "Error: " + msg );
        if ( verbose )
        {
            throwable.printStackTrace( out );
        }
    }

    @Override
    public void setTotalSteps( long steps )
    {
        this.totalSteps = String.valueOf( steps );
    }
}
