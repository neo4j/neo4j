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
package org.neo4j.kernel.info;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.function.Consumer;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.info.DiagnosticsExtractor.VisitableDiagnostics;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

public class DiagnosticsManager implements Iterable<DiagnosticsProvider>, Lifecycle
{
    private final List<DiagnosticsProvider> providers = new CopyOnWriteArrayList<DiagnosticsProvider>();
    private final Log targetLog;
    private volatile State state = State.INITIAL;

    public DiagnosticsManager( Log targetLog )
    {
        this.targetLog = targetLog;

        providers.add( new DiagnosticsProvider(/*self*/)
        {
            @Override
            public String getDiagnosticsIdentifier()
            {
                return DiagnosticsManager.this.getClass().getName();
            }

            @Override
            public void dump( DiagnosticsPhase phase, final Logger logger )
            {
                if ( phase.isInitialization() || phase.isExplicitlyRequested() )
                {
                    logger.log( "Diagnostics providers:" );
                    for ( DiagnosticsProvider provider : providers )
                    {
                        logger.log( provider.getDiagnosticsIdentifier() );
                    }
                }
            }

            @Override
            public void acceptDiagnosticsVisitor( Object visitor )
            {
                Visitor<? super DiagnosticsProvider, ? extends RuntimeException> target =
                        Visitor.SafeGenerics.castOrNull( DiagnosticsProvider.class, RuntimeException.class, visitor );
                if ( target != null ) for ( DiagnosticsProvider provider : providers )
                {
                    target.visit( provider );
                }
            }
        } );
        SystemDiagnostics.registerWith( this );
    }

    @Override
    public void init()
        throws Throwable
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.startup( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.INITIALIZED, getTargetLog() );
    }

    public void start()
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.startup( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.STARTED, getTargetLog() );
    }

    @Override
    public void stop()
        throws Throwable
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.shutdown( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.STOPPING, getTargetLog() );
        providers.clear();
    }

    public void shutdown()
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.shutdown( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.SHUTDOWN, getTargetLog() );
        providers.clear();
    }

    private enum State
    {
        INITIAL
        {
            @Override
            boolean startup( DiagnosticsManager manager )
            {
                manager.state = STARTED;
                return true;
            }
        },
        STARTED,
        STOPPED
        {
            @Override
            boolean shutdown( DiagnosticsManager manager )
            {
                return false;
            }
        };

        boolean startup( DiagnosticsManager manager )
        {
            return false;
        }

        boolean shutdown( DiagnosticsManager manager )
        {
            manager.state = STOPPED;
            return true;
        }
    }

    public Log getTargetLog()
    {
        return targetLog;
    }

    public void dumpAll()
    {
        dumpAll( DiagnosticsPhase.REQUESTED, getTargetLog() );
    }

    public void dump( String identifier )
    {
        extract( identifier, getTargetLog() );
    }

    public void dumpAll( Log log )
    {
        log.bulk( new Consumer<Log>()
        {
            @Override
            public void accept( Log bulkLog )
            {
                for ( DiagnosticsProvider provider : providers )
                {
                    dump( provider, DiagnosticsPhase.EXPLICIT, bulkLog );
                }
            }
        } );
    }

    public void extract( final String identifier, Log log )
    {
        log.bulk( new Consumer<Log>()
        {
            @Override
            public void accept( Log bulkLog )
            {
                for ( DiagnosticsProvider provider : providers )
                {
                    if ( identifier.equals( provider.getDiagnosticsIdentifier() ) )
                    {
                        dump( provider, DiagnosticsPhase.EXPLICIT, bulkLog );
                        return;
                    }
                }
            }
        } );
    }

    private void dumpAll( final DiagnosticsPhase phase, Log log )
    {
        log.bulk( new Consumer<Log>()
        {
            @Override
            public void accept( Log bulkLog )
            {
                phase.emitStart( bulkLog );
                for ( DiagnosticsProvider provider : providers )
                {
                    dump( provider, phase, bulkLog );
                }
                phase.emitDone( bulkLog );
            }
        } );
    }

    public <T> void register( DiagnosticsExtractor<T> extractor, T source )
    {
        appendProvider( extractedProvider( extractor, source ) );
    }

    public <T, E extends Enum<E> & DiagnosticsExtractor<T>> void registerAll( Class<E> extractorEnum, T source )
    {
        for ( DiagnosticsExtractor<T> extractor : extractorEnum.getEnumConstants() )
        {
            register( extractor, source );
        }
    }

    public void prependProvider( DiagnosticsProvider provider )
    {
        @SuppressWarnings( "hiding" ) State state = this.state;
        if ( state == State.STOPPED ) return;
        providers.add( 0, provider );
        if ( state == State.STARTED ) dump( DiagnosticsPhase.STARTED, provider, getTargetLog() );
    }

    public void appendProvider( DiagnosticsProvider provider )
    {
        @SuppressWarnings( "hiding" ) State state = this.state;
        if ( state == State.STOPPED ) return;
        providers.add( provider );
        if ( state == State.STARTED ) dump( DiagnosticsPhase.STARTED, provider, getTargetLog() );
    }

    private void dump( DiagnosticsPhase phase, DiagnosticsProvider provider, Log log )
    {
        phase.emitStart( log, provider );
        dump( provider, phase, log );
        phase.emitDone( log, provider );
    }

    private static void dump( DiagnosticsProvider provider, DiagnosticsPhase phase, Log log )
    {
        try
        {
            provider.dump( phase, log.infoLogger() );
        }
        catch ( Exception cause )
        {
            log.error( "Failure while logging diagnostics for " + provider, cause );
        }
    }

    @Override
    public Iterator<DiagnosticsProvider> iterator()
    {
        return providers.iterator();
    }

    static <T> DiagnosticsProvider extractedProvider( DiagnosticsExtractor<T> extractor, T source )
    {
        if ( extractor instanceof DiagnosticsExtractor.VisitableDiagnostics<?> )
        {
            return new ExtractedVisitableDiagnosticsProvider<T>(
                    (DiagnosticsExtractor.VisitableDiagnostics<T>) extractor, source );
        }
        else
        {
            return new ExtractedDiagnosticsProvider<T>( extractor, source );
        }
    }

    private static class ExtractedDiagnosticsProvider<T> implements DiagnosticsProvider
    {
        final DiagnosticsExtractor<T> extractor;
        final T source;

        ExtractedDiagnosticsProvider( DiagnosticsExtractor<T> extractor, T source )
        {
            this.extractor = extractor;
            this.source = source;
        }

        @Override
        public String getDiagnosticsIdentifier()
        {
            return extractor.toString();
        }

        @Override
        public void acceptDiagnosticsVisitor( Object visitor )
        {
            // nobody visits the source of this
        }

        @Override
        public void dump( DiagnosticsPhase phase, Logger logger )
        {
            extractor.dumpDiagnostics( source, phase, logger );
        }
    }

    private static class ExtractedVisitableDiagnosticsProvider<T> extends ExtractedDiagnosticsProvider<T>
    {
        ExtractedVisitableDiagnosticsProvider( VisitableDiagnostics<T> extractor, T source )
        {
            super( extractor, source );
        }

        @Override
        public void acceptDiagnosticsVisitor( Object visitor )
        {
            ( (DiagnosticsExtractor.VisitableDiagnostics<T>) extractor ).dispatchDiagnosticsVisitor( source, visitor );
        }
    }
}
