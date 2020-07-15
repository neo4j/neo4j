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
package org.neo4j.internal.diagnostics;

import org.neo4j.logging.Log;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.center;

/**
 * Manager that dumps information from independent available {@link DiagnosticsProvider}s.
 * Each independent diagnostics provider will be logged as table with caption and body, where caption will contain provider name
 * and body - information provided by diagnostic provider.
 */
public final class DiagnosticsManager
{
    private static final int CAPTION_WIDTH = 80;
    private static final String NAME_START = "[ ";
    private static final String NAME_END = " ]";

    private DiagnosticsManager( )
    {
    }

    public static <E extends Enum & DiagnosticsProvider> void dump( Class<E> enumProvider, Log errorLog, DiagnosticsLogger diagnosticsLog )
    {
        for ( E provider : enumProvider.getEnumConstants() )
        {
            dump( provider, errorLog, diagnosticsLog );
        }
    }

    public static void dump( DiagnosticsProvider provider, Log errorLog, DiagnosticsLogger diagnosticsLog )
    {
        try
        {
            header( diagnosticsLog, provider.getDiagnosticsName() );
            provider.dump( diagnosticsLog );
            diagnosticsLog.log( EMPTY );
        }
        catch ( Exception cause )
        {
            errorLog.error( "Failure while logging diagnostics for " + provider, cause );
        }
    }

    public static void section( DiagnosticsLogger diagnosticsLog, String sectionName )
    {
        diagnosticsLog.log( "*".repeat( CAPTION_WIDTH ) );
        diagnosticsLog.log( center( title( sectionName ), CAPTION_WIDTH ) );
        diagnosticsLog.log( "*".repeat( CAPTION_WIDTH ) );
    }

    private static void header( DiagnosticsLogger diagnosticsLog, String caption )
    {
        diagnosticsLog.log( "-".repeat( CAPTION_WIDTH ) );
        diagnosticsLog.log( center( title( caption ), CAPTION_WIDTH ) );
        diagnosticsLog.log( "-".repeat( CAPTION_WIDTH ) );
    }

    private static String title( String name )
    {
        return NAME_START + name + NAME_END;
    }
}
