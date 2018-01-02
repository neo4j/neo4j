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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

public abstract class ProgressMonitorFactory
{
    public static final ProgressMonitorFactory NONE = new ProgressMonitorFactory()
    {
        @Override
        protected Indicator newIndicator( String process )
        {
            return Indicator.NONE;
        }

        @Override
        protected Indicator.OpenEnded newOpenEndedIndicator( String process, int resolution )
        {
            return Indicator.NONE;
        }
    };

    public static ProgressMonitorFactory textual( final OutputStream out )
    {
        try
        {
            return textual( new OutputStreamWriter( out, "UTF-8" ) );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static ProgressMonitorFactory textual( final Writer out )
    {
        return new ProgressMonitorFactory()
        {
            @Override
            protected Indicator newIndicator( String process )
            {
                return new Indicator.Textual( process, writer() );
            }

            @Override
            protected Indicator.OpenEnded newOpenEndedIndicator( String process, int resolution )
            {
                return new Indicator.OpenEndedTextual( process, writer(), resolution );
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

    public final ProgressListener openEnded( String process, int resolution )
    {
        return new ProgressListener.OpenEndedProgressListener( newOpenEndedIndicator( process, resolution ) );
    }

    protected abstract Indicator newIndicator( String process );

    protected abstract Indicator.OpenEnded newOpenEndedIndicator( String process, int resolution );

    public static class MultiPartBuilder
    {
        private Aggregator aggregator;
        private Set<String> parts = new HashSet<String>();
        private Completion completion = null;

        private MultiPartBuilder( ProgressMonitorFactory factory, String process )
        {
            this.aggregator = new Aggregator(factory.newIndicator( process ));
        }

        public ProgressListener progressForPart( String part, long totalCount )
        {
            if ( aggregator == null )
            {
                throw new IllegalStateException( "Builder has been completed." );
            }
            if ( !parts.add( part ) )
            {
                throw new IllegalArgumentException( String.format( "Part '%s' has already been defined.", part ) );
            }
            ProgressListener.MultiPartProgressListener progress = new ProgressListener.MultiPartProgressListener( aggregator, part, totalCount );
            aggregator.add( progress );
            return progress;
        }

        public Completion build()
        {
            if ( aggregator != null )
            {
                completion = aggregator.initialize();
            }
            aggregator = null;
            parts = null;
            return completion;
        }
    }
}
