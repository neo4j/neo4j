/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal.diagnostics;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.diagnostics.DiagnosticsExtractor.VisitableDiagnostics;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLog;

/**
 * Collects and manages all {@link DiagnosticsProvider}.
 */
public class DiagnosticsManager implements Iterable<DiagnosticsProvider>
{
    private final List<DiagnosticsProvider> providers = new CopyOnWriteArrayList<>();
    private final Log targetLog;

    public DiagnosticsManager( Log targetLog )
    {
        this.targetLog = targetLog;

        providers.add( new DiagnosticsProvider()
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
                if ( target != null )
                {
                    for ( DiagnosticsProvider provider : providers )
                    {
                        target.visit( provider );
                    }
                }
            }
        } );
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

    public void dump( DiagnosticsProvider diagnosticsProvider )
    {
        dump( diagnosticsProvider, DiagnosticsPhase.EXPLICIT, getTargetLog() );
    }

    public <T, E extends Enum<E> & DiagnosticsExtractor<T>> void dump( Class<E> extractorEnum, T source )
    {
        for ( DiagnosticsExtractor<T> extractor : extractorEnum.getEnumConstants() )
        {
            dump( extractedProvider( extractor, source ) );
        }
    }

    public void dumpAll( Log log )
    {
        log.bulk( bulkLog ->
        {
            for ( DiagnosticsProvider provider : providers )
            {
                dump( provider, DiagnosticsPhase.EXPLICIT, bulkLog );
            }
        } );
    }

    public void extract( final String identifier, Log log )
    {
        log.bulk( bulkLog ->
        {
            for ( DiagnosticsProvider provider : providers )
            {
                if ( identifier.equals( provider.getDiagnosticsIdentifier() ) )
                {
                    dump( provider, DiagnosticsPhase.EXPLICIT, bulkLog );
                    return;
                }
            }
        } );
    }

    private void dumpAll( final DiagnosticsPhase phase, Log log )
    {
        log.bulk( bulkLog ->
        {
            phase.emitStart( bulkLog );
            for ( DiagnosticsProvider provider : providers )
            {
                dump( provider, phase, bulkLog );
            }
            phase.emitDone( bulkLog );
        } );
    }

    public <E extends Enum & DiagnosticsProvider> void dump( Class<E> enumProvider )
    {
        for ( E provider : enumProvider.getEnumConstants() )
        {
            dump( provider );
        }
    }

    public <T> void register( DiagnosticsExtractor<T> extractor, T source )
    {
        appendProvider( extractedProvider( extractor, source ) );
    }

    public void appendProvider( DiagnosticsProvider provider )
    {
        providers.add( provider );
        dump( DiagnosticsPhase.STARTED, provider, getTargetLog() );
    }

    private void dump( DiagnosticsPhase phase, DiagnosticsProvider provider, Log log )
    {
        phase.emitStart( log, provider );
        dump( provider, phase, log );
        phase.emitDone( log, provider );
    }

    private static void dump( DiagnosticsProvider provider, DiagnosticsPhase phase, Log log )
    {
        // Optimization to skip diagnostics dumping (which is time consuming) if there's no log anyway.
        // This is first and foremost useful for speeding up testing.
        if ( log == NullLog.getInstance() )
        {
            return;
        }

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

    private static <T> DiagnosticsProvider extractedProvider( DiagnosticsExtractor<T> extractor, T source )
    {
        if ( extractor instanceof DiagnosticsExtractor.VisitableDiagnostics<?> )
        {
            return new ExtractedVisitableDiagnosticsProvider<>(
                    (DiagnosticsExtractor.VisitableDiagnostics<T>) extractor, source );
        }
        else
        {
            return new ExtractedDiagnosticsProvider<>( extractor, source );
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
