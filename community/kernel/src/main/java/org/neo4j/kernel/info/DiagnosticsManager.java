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
package org.neo4j.kernel.info;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor.VisitableDiagnostics;

public final class DiagnosticsManager implements Iterable<DiagnosticsProvider>, Lifecycle
{
    @SuppressWarnings( "unchecked" )
    public static final <T> Visitor<? super T> castToGenericVisitor( Class<T> type, Object visitor )
    {
        if ( visitor instanceof Visitor<?> )
        {
            for ( Type iface : visitor.getClass().getGenericInterfaces() )
            {
                if ( iface instanceof ParameterizedType )
                {
                    ParameterizedType paramType = (ParameterizedType) iface;
                    if ( paramType.getRawType() == Visitor.class )
                    {
                        return (Visitor<? super T>) visitor;
                    }
                }
            }
        }
        return null;
    }

    private final List<DiagnosticsProvider> providers = new CopyOnWriteArrayList<DiagnosticsProvider>();
    private final StringLogger logger;
    private volatile State state = State.INITIAL;

    public DiagnosticsManager( StringLogger logger )
    {
        ( this.logger = logger ).addRotationListener( new Runnable()
        {
            @Override
            public void run()
            {
                dumpAll( DiagnosticsPhase.LOG_ROTATION );
            }
        } );
        providers.add( new DiagnosticsProvider(/*self*/)
        {
            @Override
            public String getDiagnosticsIdentifier()
            {
                return DiagnosticsManager.this.getClass().getName();
            }

            @Override
            public void dump( DiagnosticsPhase phase, final StringLogger log )
            {
                if ( phase.isInitialization() || phase.isExplicitlyRequested() )
                {
                    log.logLongMessage( "Diagnostics providers:", new IterableWrapper<String, DiagnosticsProvider>(
                            providers )
                    {
                        @Override
                        protected String underlyingObjectToObject( DiagnosticsProvider provider )
                        {
                            return provider.getDiagnosticsIdentifier();
                        }
                    }, true );
                }
            }

            @Override
            public void acceptDiagnosticsVisitor( Object visitor )
            {
                Visitor<? super DiagnosticsProvider> target = castToGenericVisitor( DiagnosticsProvider.class, visitor );
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
    }

    public void start()
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.startup( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.STARTUP );
    }

    @Override
    public void stop()
        throws Throwable
    {
    }

    public void shutdown()
    {
        synchronized ( providers )
        {
            @SuppressWarnings( "hiding" ) State state = this.state;
            if ( !state.shutdown( this ) ) return;
        }
        dumpAll( DiagnosticsPhase.SHUTDOWN );
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

    public StringLogger getTargetLog()
    {
        return logger;
    }

    public void dumpAll()
    {
        dumpAll( DiagnosticsPhase.REQUESTED );
    }

    public void dump( String identifier )
    {
        extract( identifier, logger );
    }

    public void extract( String identifier, StringLogger log )
    {
        for ( DiagnosticsProvider provider : providers )
        {
            if ( identifier.equals( provider.getDiagnosticsIdentifier() ) )
            {
                dump( provider, DiagnosticsPhase.EXPLICIT, log );
                return;
            }
        }
    }

    public void visitAll( Object visitor )
    {
        for ( DiagnosticsProvider provider : providers )
        {
            provider.acceptDiagnosticsVisitor( visitor );
        }
    }

    private void dumpAll( DiagnosticsPhase phase )
    {
        phase.emitStart( logger );
        for ( DiagnosticsProvider provider : providers )
        {
            dump( provider, phase, logger );
        }
        phase.emitDone( logger );
    }

    public <T> void register( DiagnosticsExtractor<T> extractor, T source )
    {
        appendProvider( extractedProvider( extractor, source ) );
    }

    public <T> T tryAppendProvider( T protentialProvider )
    {
        if ( protentialProvider instanceof DiagnosticsProvider )
        {
            appendProvider( (DiagnosticsProvider) protentialProvider );
        }
        return protentialProvider;
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
        if ( state == State.STARTED ) dump( DiagnosticsPhase.STARTUP, provider );
    }

    public void appendProvider( DiagnosticsProvider provider )
    {
        @SuppressWarnings( "hiding" ) State state = this.state;
        if ( state == State.STOPPED ) return;
        providers.add( provider );
        if ( state == State.STARTED ) dump( DiagnosticsPhase.STARTUP, provider );
    }

    private void dump( DiagnosticsPhase phase, DiagnosticsProvider provider )
    {
        phase.emitStart( logger, provider );
        dump( provider, phase, logger );
        phase.emitDone( logger, provider );
    }

    private static void dump( DiagnosticsProvider provider, DiagnosticsPhase phase, StringLogger logger )
    {
        try
        {
            provider.dump( phase, logger );
        }
        catch ( Exception cause )
        {
            logger.logMessage( "Failure while logging diagnostics for " + provider, cause );
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
        public void dump( DiagnosticsPhase phase, StringLogger log )
        {
            extractor.dumpDiagnostics( source, phase, log );
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
