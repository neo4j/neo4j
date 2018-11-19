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

import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

// TODO:javadoc
public class DiagnosticsManager
{
    private final Log log;

    public DiagnosticsManager( Log log )
    {
        this.log = log;
    }

    public void dump( DiagnosticsProvider provider )
    {
        dump( provider, getLog() );
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
        dump( enumProvider, getLog() );
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
            provider.dump( log.infoLogger() );
        }
        catch ( Exception cause )
        {
            log.error( "Failure while logging diagnostics for " + provider, cause );
        }
    }

    public Log getLog()
    {
        return log;
    }
}
