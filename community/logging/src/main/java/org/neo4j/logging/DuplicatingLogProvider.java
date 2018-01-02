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
package org.neo4j.logging;

import org.neo4j.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A {@link LogProvider} implementation that duplicates all messages to other LogProvider instances
 */
public class DuplicatingLogProvider extends AbstractLogProvider<DuplicatingLog>
{
    private final CopyOnWriteArraySet<LogProvider> logProviders;
    private final Map<DuplicatingLog, Map<LogProvider, Log>> duplicatingLogCache = Collections.synchronizedMap( new WeakHashMap<DuplicatingLog, Map<LogProvider, Log>>() );

    /**
     * @param logProviders A list of {@link LogProvider} instances that messages should be duplicated to
     */
    public DuplicatingLogProvider( LogProvider... logProviders )
    {
        this.logProviders = new CopyOnWriteArraySet<>( Arrays.asList( logProviders ) );
    }

    /**
     * Remove a {@link LogProvider} from the duplicating set. Note that the LogProvider must return
     * cached Log instances from its {@link LogProvider#getLog(String)} for this to behave as expected.
     *
     * @param logProvider the LogProvider to be removed
     * @return true if the log was found and removed
     */
    public boolean remove( LogProvider logProvider )
    {
        if ( !this.logProviders.remove( logProvider ) )
        {
            return false;
        }
        for ( DuplicatingLog duplicatingLog : cachedLogs() )
        {
            duplicatingLog.remove( duplicatingLogCache.get( duplicatingLog ).remove( logProvider ) );
        }
        return true;
    }

    @Override
    protected DuplicatingLog buildLog( final Class loggingClass )
    {
        return buildLog( new Function<LogProvider, Log>()
        {
            @Override
            public Log apply( LogProvider logProvider )
            {
                return logProvider.getLog( loggingClass );
            }
        } );
    }

    @Override
    protected DuplicatingLog buildLog( final String name )
    {
        return buildLog( new Function<LogProvider, Log>()
        {
            @Override
            public Log apply( LogProvider logProvider )
            {
                return logProvider.getLog( name );
            }
        } );
    }

    private DuplicatingLog buildLog( Function<LogProvider, Log> logConstructor )
    {
        ArrayList<Log> logs = new ArrayList<>( logProviders.size() );
        HashMap<LogProvider, Log> providedLogs = new HashMap<>();
        for ( LogProvider logProvider : logProviders )
        {
            Log log = logConstructor.apply( logProvider );
            providedLogs.put( logProvider, log );
            logs.add( log );
        }
        DuplicatingLog duplicatingLog = new DuplicatingLog( logs );
        duplicatingLogCache.put( duplicatingLog, providedLogs );
        return duplicatingLog;
    }
}
