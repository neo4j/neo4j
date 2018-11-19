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

import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLog;

/**
 * Collects and manages all {@link DiagnosticsProvider}.
 */
public class DiagnosticsManager implements Iterable<DiagnosticsProvider>
{
    private final List<DiagnosticsProvider> providers = new CopyOnWriteArrayList<>();
    private final Log log;

    public DiagnosticsManager( Log log )
    {
        this.log = log;

        providers.add( new DiagnosticsProvider()
        {
            @Override
            public String getDiagnosticsIdentifier()
            {
                return DiagnosticsManager.this.getClass().getName();
            }

            @Override
            public void dump( final Logger logger )
            {
                logger.log( "Diagnostics providers:" );
                for ( DiagnosticsProvider provider : providers )
                {
                    logger.log( provider.getDiagnosticsIdentifier() );
                }
            }
        } );
    }

    private Log getLog()
    {
        return log;
    }

    public void dumpAll()
    {
        dumpAll( getLog() );
    }

    public void dump( String identifier )
    {
        extract( identifier, getLog() );
    }

    public void dump( DiagnosticsProvider diagnosticsProvider )
    {
        dump( diagnosticsProvider, getLog() );
    }

    public void dumpAll( Log log )
    {
        log.bulk( bulkLog ->
        {
            for ( DiagnosticsProvider provider : providers )
            {
                dump( provider, bulkLog );
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
                    dump( provider, bulkLog );
                    return;
                }
            }
        } );
    }

    public <E extends Enum & DiagnosticsProvider> void dump( Class<E> enumProvider )
    {
        for ( E provider : enumProvider.getEnumConstants() )
        {
            dump( provider );
        }
    }

    private static void dump( DiagnosticsProvider provider, Log log )
    {
        // Optimization to skip diagnostics dumping (which is time consuming) if there's no log anyway.
        // This is first and foremost useful for speeding up testing.
        if ( log == NullLog.getInstance() )
        {
            return;
        }

        try
        {
            provider.dump( log.infoLogger() );
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
}
