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
package org.neo4j.internal.diagnostics;

import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.center;
import static org.apache.commons.lang3.StringUtils.repeat;

/**
 * Manager that dumps information from independent available {@link DiagnosticsProvider}s.
 * Each independent diagnostics provider will be logged as table with caption and body, where caption will contain provider name
 * and body - information provided by diagnostic provider.
 */
public class DiagnosticsManager
{
    private static final int CAPTION_WIDTH = 80;
    private static final String NAME_START = "[ ";
    private static final String NAME_END = " ]";

    private final Log log;

    public DiagnosticsManager( Log log )
    {
        this.log = log;
    }

    public void dump( DiagnosticsProvider provider )
    {
        dump( provider, log );
    }

    public void dump( List<DiagnosticsProvider> providers, Log dumpLog )
    {
        dumpLog.bulk( bulkLog ->
        {
            for ( DiagnosticsProvider provider : providers )
            {
                dump( provider, bulkLog );
            }
        } );
    }

    public <E extends Enum & DiagnosticsProvider> void dump( Class<E> enumProvider )
    {
        dump( enumProvider, log );
    }

    public <E extends Enum & DiagnosticsProvider> void dump( Class<E> enumProvider, Log log )
    {
        for ( E provider : enumProvider.getEnumConstants() )
        {
            dump( provider, log );
        }
    }

    public void dump( DiagnosticsProvider provider, Log log )
    {
        // Optimization to skip diagnostics dumping (which is time consuming) if there's no log anyway.
        // This is first and foremost useful for speeding up testing.
        if ( log == NullLog.getInstance() )
        {
            return;
        }
        try
        {
            header( log, provider.getDiagnosticsName() );
            provider.dump( log.infoLogger() );
            log.info( EMPTY );
        }
        catch ( Exception cause )
        {
            log.error( "Failure while logging diagnostics for " + provider, cause );
        }
    }

    public void section( Log log, String sectionName )
    {
        log.info( repeat( "*", CAPTION_WIDTH ) );
        log.info( center( title( sectionName ), CAPTION_WIDTH ) );
        log.info( repeat( "*", CAPTION_WIDTH ) );
    }

    private static void header( Log log, String caption )
    {
        log.info( repeat( "-", CAPTION_WIDTH ) );
        log.info( center( title( caption ), CAPTION_WIDTH ) );
        log.info( repeat( "-", CAPTION_WIDTH ) );
    }

    private static String title( String name )
    {
        return NAME_START + name + NAME_END;
    }
}
