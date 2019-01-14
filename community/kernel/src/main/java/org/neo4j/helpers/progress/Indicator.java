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
package org.neo4j.helpers.progress;

import java.io.PrintWriter;

public abstract class Indicator
{
    public static final Indicator NONE = new Indicator( 1 )
    {
        @Override
        protected void progress( int from, int to )
        {
        }
    };

    private final int reportResolution;

    public Indicator( int reportResolution )
    {
        this.reportResolution = reportResolution;
    }

    protected abstract void progress( int from, int to );

    int reportResolution()
    {
        return reportResolution;
    }

    public void startProcess( long totalCount )
    {
    }

    public void startPart( String part, long totalCount )
    {
    }

    public void completePart( String part )
    {
    }

    public void completeProcess()
    {
    }

    public void failure( Throwable cause )
    {
    }

    static class Textual extends Indicator
    {
        private final String process;
        private final PrintWriter out;

        Textual( String process, PrintWriter out )
        {
            super( 200 );
            this.process = process;
            this.out = out;
        }

        @Override
        public void startProcess( long totalCount )
        {
            out.println( process );
            out.flush();
        }

        @Override
        protected void progress( int from, int to )
        {
            for ( int i = from; i < to; )
            {
                printProgress( ++i );
            }
            out.flush();
        }

        @Override
        public void failure( Throwable cause )
        {
            cause.printStackTrace( out );
        }

        private void printProgress( int progress )
        {
            out.print( '.' );
            if ( progress % 20 == 0 )
            {
                out.printf( " %3d%%%n", progress / 2 );
            }
        }
    }
}
