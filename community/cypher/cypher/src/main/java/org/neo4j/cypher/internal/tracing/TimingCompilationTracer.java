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
package org.neo4j.cypher.internal.tracing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer;

public class TimingCompilationTracer implements CompilationTracer
{
    public interface EventListener
    {
        void queryCompiled( QueryEvent event );
    }

    public interface QueryEvent
    {
        String query();

        long nanoTime();

        List<PhaseEvent> phases();
    }

    public interface PhaseEvent
    {
        CompilationPhaseTracer.CompilationPhase phase();

        long nanoTime();
    }

    interface Clock
    {
        long nanoTime();

        Clock SYSTEM = new Clock()
        {
            @Override
            public long nanoTime()
            {
                return System.nanoTime();
            }
        };
    }

    private final Clock clock;
    private final EventListener listener;

    public TimingCompilationTracer( EventListener listener )
    {
        this( Clock.SYSTEM, listener );
    }

    TimingCompilationTracer( Clock clock, EventListener listener )
    {
        this.clock = clock;
        this.listener = listener;
    }

    @Override
    public QueryCompilationEvent compileQuery( String query )
    {
        return new Query( clock, query, listener );
    }

    private static abstract class Event implements AutoCloseable
    {
        private Clock clock;
        private long time;

        Event( Clock clock )
        {
            this.clock = clock;
            this.time = clock.nanoTime();
        }

        @Override
        public final void close()
        {
            if ( clock != null )
            {
                time = clock.nanoTime() - time;
                clock = null;
                done();
            }
        }

        @SuppressWarnings( "UnusedDeclaration"/*used through inheritance*/ )
        public final long nanoTime()
        {
            return time;
        }

        abstract void done();

        final Clock clock()
        {
            if ( clock == null )
            {
                throw new IllegalStateException( this + " has been closed" );
            }
            return clock;
        }
    }

    private static class Query extends Event implements QueryEvent, QueryCompilationEvent
    {
        private final String query;
        private final EventListener listener;
        private final List<Phase> phases = new ArrayList<>();

        Query( Clock clock, String query, EventListener listener )
        {
            super( clock );
            this.query = query;
            this.listener = listener;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + query + "]";
        }

        @Override
        public CompilationPhaseEvent beginPhase( CompilationPhase phase )
        {
            Phase event = new Phase( clock(), phase );
            phases.add( event );
            return event;
        }

        @Override
        void done()
        {
            listener.queryCompiled( this );
        }

        @Override
        public String query()
        {
            return query;
        }

        @Override
        public List<PhaseEvent> phases()
        {
            return Collections.<PhaseEvent>unmodifiableList( phases );
        }
    }

    private static class Phase extends Event implements PhaseEvent, CompilationPhaseTracer.CompilationPhaseEvent
    {
        private final CompilationPhaseTracer.CompilationPhase phase;

        Phase( Clock clock, CompilationPhaseTracer.CompilationPhase phase )
        {
            super( clock );
            this.phase = phase;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + phase + "]";
        }

        @Override
        public CompilationPhaseTracer.CompilationPhase phase()
        {
            return phase;
        }

        @Override
        void done()
        {
        }
    }
}
