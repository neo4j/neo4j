/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.helpers;

import java.io.PrintStream;

/**
 * A generic interface for reporting progress by a tool. Can be implemented to
 * support reporting progress from both a {@link SimpleProgress single source},
 * or for the aggregate of {@link MultiProgress multiple sources}.
 * 
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 */
public interface ProgressIndicator
{
    /**
     * A factory interface for creating {@link ProgressIndicator}s.
     *
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public interface Factory
    {
        SimpleProgress newSimpleProgressIndicator(long total);

        MultiProgress newMultiProgressIndicator(long total);
    }

    /**
     * A factory implementation that creates progress indicators that log progress textually.
     *
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public final class Textual implements Factory
    {
        private final PrintStream out;

        public Textual( PrintStream out )
        {
            this.out = out;
        }

        @Override
        public SimpleProgress newSimpleProgressIndicator( long total )
        {
            return SimpleProgress.textual( out, total );
        }

        @Override
        public MultiProgress newMultiProgressIndicator( long total )
        {
            return MultiProgress.textual( out, total );
        }
    }

    /**
     * Update the current progress count for the current source.
     * 
     * @param incremental whether this is an incremental update (
     *            <code>true</code>) or an absolute assignment (
     *            <code>false</code>) of the progress.
     * @param value the count to update the progress with.
     */
    void update( boolean incremental, long value );

    /**
     * Mark the process as done with the current source.
     * 
     * @param totalProgress the total progress reached by the source.
     */
    void done( long totalProgress );

    /**
     * A {@link ProgressIndicator} that can report the progress for a single
     * source.
     * 
     * Should be initialized with the total count that the source is going to
     * reach.
     * 
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public abstract class SimpleProgress implements ProgressIndicator
    {
        private final long total;
        private int lastPermille = 0;
        private long currentProgress = 0;

        /**
         * Create a {@link ProgressIndicator} for a single source.
         * 
         * @param total the total count the process will reach.
         */
        public SimpleProgress( long total )
        {
            this.total = total;
        }

        /**
         * Returns a simple {@link ProgressIndicator} that reports progress by
         * printing to the provided stream.
         * 
         * @param out the stream to print progress indication to.
         * @param total the total count the process will reach.
         * @return a simple {@link ProgressIndicator} that reports progress by
         *         printing to the provided stream.
         */
        public static SimpleProgress textual( final PrintStream out, long total )
        {
            return new SimpleProgress( total )
            {
                @Override
                protected void progress( int permille )
                {
                    textualProgress( out, permille );
                }
            };
        }

        long currentProgress( boolean incremental, long value )
        {
            return currentProgress = ( incremental ? ( currentProgress + value ) : value );
        }

        @Override
        public void update( boolean incremental, long value )
        {
            int permille = (int) ( ( currentProgress( incremental, value ) * 1000 ) / total );
            if ( permille != lastPermille ) progress( lastPermille = permille );
        }

        @Override
        public void done( long totalProgress )
        {
            done();
        }

        void done()
        {
            if ( lastPermille < 1000 ) progress( lastPermille = 1000 );
        }

        /**
         * Implement this method to emit the progress notifications.
         * 
         * @param permille value from 0 (just started) to 1000 (done) indicating
         *            how far along the process is.
         */
        protected abstract void progress( int permille );

        static void textualProgress( PrintStream out, int permille )
        {
            if ( permille % 100 == 0 )
                out.printf( "%3s%%%n", Integer.toString( permille / 10 ) );
            else if ( permille % 5 == 0 ) out.print( "." );
        }
    }

    /**
     * A {@link ProgressIndicator} that can report the total progress for
     * multiple sources.
     * 
     * Needs to be initialized with the total count that should be reached by
     * all sources together.
     * 
     * When the entire process (all sources) is completed, {@link #done()} needs
     * to be invoked.
     * 
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public abstract class MultiProgress extends SimpleProgress
    {
        private long base = 0;

        /**
         * Create a {@link ProgressIndicator} for multiple sources.
         * 
         * @param total the total count the entire process (all sources) will
         *            reach.
         */
        public MultiProgress( long total )
        {
            super( total );
        }

        /**
         * Returns a simple {@link ProgressIndicator} that reports progress by
         * printing to the provided stream.
         * 
         * @param out the stream to print progress indication to.
         * @param total the total count the entire process (all sources) will
         *            reach.
         * @return a simple {@link ProgressIndicator} that reports progress by
         *         printing to the provided stream.
         */
        public static MultiProgress textual( final PrintStream out, long total )
        {
            return new MultiProgress( total )
            {
                @Override
                protected void progress( int permille )
                {
                    textualProgress( out, permille );
                }
            };
        }

        @Override
        long currentProgress( boolean incremental, long value )
        {
            return base + super.currentProgress( incremental, value );
        }

        @Override
        public void done( long totalSegmentProgress )
        {
            update( false, totalSegmentProgress );
            base += totalSegmentProgress;
            currentProgress( false, 0 ); // reset for the next count
        }

        /**
         * Signal that the entire progress (all sources) is done.
         */
        @Override
        public void done()
        {
            super.done();
        }
    }
    
    /**
     * Progress indicator where the end is unknown. Specify a step size which
     * means that a means of progress will be printed every step.
     * 
     * @author Mattias Persson
     */
    public class UnknownEndProgress implements ProgressIndicator
    {
        private final long stepSize;
        private long lastAbsolutePosition = -1;
        private long position;
        private long lastStep;
        private final String doneMessage;
        private final PrintStream out;

        public UnknownEndProgress( long stepSize, String doneMessage )
        {
            this(stepSize, doneMessage, new PrintStream(System.out));
        }
        
        public UnknownEndProgress(long stepSize, String doneMessage, PrintStream out)
        {
            this.stepSize = stepSize;
            this.doneMessage = doneMessage;
            this.out = out;
        }

        @Override
        public void update( boolean incremental, long value )
        {
            position += incremental ? updateIncremental( value ) : updateAbsolute( value );
            long step = position/stepSize;
            if ( lastStep != step )
            {
                if ( lastStep > 0 && lastStep % 30 == 0 ) out.println();
                out.print( "." );
            }
            lastStep = step;
        }

        private long updateIncremental( long value )
        {
            return value;
        }

        private long updateAbsolute( long value )
        {
            if ( lastAbsolutePosition == -1 ) lastAbsolutePosition = value;
            try
            {
                return value - lastAbsolutePosition;
            }
            finally
            {
                lastAbsolutePosition = value;
            }
        }

        @Override
        public void done( long totalProgress )
        {
            if ( lastStep > 0 ) out.println();
            out.println( "[" + totalProgress + " " + doneMessage + "]" );
        }
    }
}
