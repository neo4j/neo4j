/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.helpers.progress;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.time.Clocks;
import org.neo4j.util.Preconditions;

public abstract class ProgressMonitorFactory
{
    public static final ProgressMonitorFactory NONE = new ProgressMonitorFactory()
    {
        @Override
        protected Indicator newIndicator( String process )
        {
            return Indicator.NONE;
        }
    };

    public static ProgressMonitorFactory textual( final OutputStream out )
    {
        return textual( new OutputStreamWriter( out, StandardCharsets.UTF_8 ), false );
    }

    @Deprecated
    public static ProgressMonitorFactory textualWithDeltaTimes( final OutputStream out )
    {
        return textual( new OutputStreamWriter( out, StandardCharsets.UTF_8 ), true );
    }

    @Deprecated
    public static ProgressMonitorFactory textualWithDeltaTimes( final Writer out )
    {
        return textual( out, true );
    }

    public static ProgressMonitorFactory textual( final Writer out )
    {
        return textual( out, false );
    }

    @Deprecated
    private static ProgressMonitorFactory textual( final Writer out, boolean deltaTimes )
    {
        return new ProgressMonitorFactory()
        {
            @Override
            protected Indicator newIndicator( String process )
            {
                return new Indicator.Textual( process, writer(), deltaTimes, Clocks.nanoClock(), Indicator.Textual.DEFAULT_DELTA_CHARACTER );
            }

            private PrintWriter writer()
            {
                return out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter( out );
            }
        };
    }

    public final MultiPartBuilder multipleParts( String process )
    {
        return new MultiPartBuilder( this, process );
    }

    public final ProgressListener singlePart( String process, long totalCount )
    {
        return new ProgressListener.SinglePartProgressListener( newIndicator( process ), totalCount );
    }

    protected abstract Indicator newIndicator( String process );

    public static class MultiPartBuilder
    {
        private Aggregator aggregator;
        private Set<String> parts = new HashSet<>();

        private MultiPartBuilder( ProgressMonitorFactory factory, String process )
        {
            this.aggregator = new Aggregator( factory.newIndicator( process ) );
        }

        public ProgressListener progressForPart( String part, long totalCount )
        {
            assertNotBuilt();
            assertUniquePart( part );
            ProgressListener.MultiPartProgressListener progress =
                    new ProgressListener.MultiPartProgressListener( aggregator, part, totalCount );
            aggregator.add( progress, totalCount );
            return progress;
        }

        public ProgressListener progressForUnknownPart( String part )
        {
            assertNotBuilt();
            assertUniquePart( part );
            ProgressListener progress = ProgressListener.NONE;
            aggregator.add( progress, 0 );
            return progress;
        }

        private void assertUniquePart( String part )
        {
            if ( !parts.add( part ) )
            {
                throw new IllegalArgumentException( String.format( "Part '%s' has already been defined.", part ) );
            }
        }

        private void assertNotBuilt()
        {
            if ( aggregator == null )
            {
                throw new IllegalStateException( "Builder has been completed." );
            }
        }

        /**
         * Have to be called after all individual progresses have been added.
         * @return a {@link Completer} which can be called do issue {@link ProgressListener#done()} for all individual progress parts.
         */
        public Completer build()
        {
            Preconditions.checkState( aggregator != null, "Already built" );
            Completer completer = aggregator.initialize();
            aggregator = null;
            parts = null;
            return completer;
        }

        /**
         * Can be called to invoke all individual {@link ProgressListener#done()}.
         */
        public void done()
        {
            aggregator.done();
        }
    }

    public interface Completer extends AutoCloseable
    {
        @Override
        void close();
    }
}
