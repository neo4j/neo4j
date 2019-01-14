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
import java.util.Collections;

/**
 * Tracks progress in an interactive way, relies on the fact that the {@code PrintStream} echoes to a terminal that can
 * interpret the carrier return to reset the current line.
 */
public class InteractiveProgress implements DiagnosticsReporterProgress
{
    private String prefix;
    private String suffix;
    private String totalSteps = "?";
    private final PrintStream out;
    private final boolean verbose;
    private String info = "";
    private int longestInfo;

    public InteractiveProgress( PrintStream out, boolean verbose )
    {
        this.out = out;
        this.verbose = verbose;
    }

    @Override
    public void percentChanged( int percent )
    {
        out.print( String.format( "\r%8s [", prefix ) );
        int totalWidth = 20;

        int numBars = totalWidth * percent / 100;
        for ( int i = 0; i < totalWidth; i++ )
        {
            if ( i < numBars )
            {
                out.print( '#' );
            }
            else
            {
                out.print( ' ' );
            }
        }
        out.print( String.format( "] %3s%%   %s %s", percent, suffix, info ) );
    }

    @Override
    public void started( long currentStepIndex, String target )
    {
        this.prefix = currentStepIndex + "/" + totalSteps;
        this.suffix = target;
        percentChanged( 0 );
    }

    @Override
    public void finished()
    {
        // Pad string to erase info string
        info = String.join( "", Collections.nCopies( longestInfo, " " ) );

        percentChanged( 100 );
        out.println();
    }

    @Override
    public void info( String info )
    {
        this.info = info;
        if ( info.length() > longestInfo )
        {
            longestInfo = info.length();
        }
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
