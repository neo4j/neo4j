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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
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
    };

    public static ProgressMonitorFactory textual( final OutputStream out )
    {
        return textual( new OutputStreamWriter( out, StandardCharsets.UTF_8 ) );
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

        public void build()
        {
            if ( aggregator != null )
            {
                aggregator.initialize();
            }
            aggregator = null;
            parts = null;
        }
    }
}
