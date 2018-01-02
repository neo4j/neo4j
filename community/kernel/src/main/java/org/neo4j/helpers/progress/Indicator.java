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
package org.neo4j.helpers.progress;

import java.io.PrintWriter;

public abstract class Indicator
{
    static final Indicator.OpenEnded NONE = new Indicator.OpenEnded( 1 )
    {
        @Override
        protected void progress( int from, int to )
        {
            // do nothing
        }
    };

    public static abstract class OpenEnded extends Indicator
    {
        public OpenEnded( int reportResolution )
        {
            super( reportResolution );
        }
    }

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
        // default: do nothing
    }

    public void startPart( String part, long totalCount )
    {
        // default: do nothing
    }

    public void completePart( String part )
    {
        // default: do nothing
    }

    public void completeProcess()
    {
        // default: do nothing
    }

    public void failure( Throwable cause )
    {
        // default: do nothing
    }

    public static abstract class Decorator extends Indicator.OpenEnded
    {
        private final Indicator indicator;

        /** Constructor for regular indicators. */
        public Decorator( ProgressMonitorFactory factory, String process )
        {
            this( factory.newIndicator( process ) );
        }

        /** Constructor for open ended indicators. */
        public Decorator( ProgressMonitorFactory factory, String process, int resolution )
        {
            this( factory.newOpenEndedIndicator( process, resolution ) );
        }

        public Decorator( Indicator indicator )
        {
            super( indicator.reportResolution() );
            this.indicator = indicator;
        }

        @Override
        public void startProcess( long totalCount )
        {
            indicator.startProcess( totalCount );
        }

        @Override
        public void startPart( String part, long totalCount )
        {
            indicator.startPart( part, totalCount );
        }

        @Override
        public void completePart( String part )
        {
            indicator.completePart( part );
        }

        @Override
        public void completeProcess()
        {
            indicator.completeProcess();
        }

        @Override
        protected void progress( int from, int to )
        {
            indicator.progress( from, to );
        }

        @Override
        public void failure( Throwable cause )
        {
            indicator.failure( cause );
        }
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

    static class OpenEndedTextual extends OpenEnded
    {
        private final String process;
        private final PrintWriter out;
        private int dots;

        OpenEndedTextual( String process, PrintWriter out, int reportResolution )
        {
            super( reportResolution );
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
        public void completeProcess()
        {
            for ( int i = dots; i < 20; i++ )
            {
                out.print( " " );
            }
            out.println( "    done" );
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
            dots++;
            if ( progress % 20 == 0 )
            {
                out.printf( " %7d%n", ((long) progress) * reportResolution() );
                dots = 0;
            }
        }
    }
}
