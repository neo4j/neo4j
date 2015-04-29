/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.logging;

import org.neo4j.function.Consumer;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Log} implementation that duplicates all messages to other Log instances
 */
public class DuplicatingLog extends AbstractLog
{
    private final Log[] logs;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    /**
     * @param logs A list of {@link Log} instances that messages should be duplicated to
     */
    public DuplicatingLog( List<Log> logs )
    {
        this( logs.toArray( new Log[logs.size()] ) );
    }

    /**
     * @param logs A list of {@link Log} instances that messages should be duplicated to
     */
    public DuplicatingLog( Log... logs )
    {
        this.logs = logs;
        ArrayList<Logger> debugLoggers = new ArrayList<>();
        ArrayList<Logger> infoLoggers = new ArrayList<>();
        ArrayList<Logger> warnLoggers = new ArrayList<>();
        ArrayList<Logger> errorLoggers = new ArrayList<>();

        for ( Log log : logs )
        {
            debugLoggers.add( log.debugLogger() );
            infoLoggers.add( log.infoLogger() );
            warnLoggers.add( log.warnLogger() );
            errorLoggers.add( log.errorLogger() );
        }

        this.debugLogger = new DuplicatingLogger( debugLoggers );
        this.infoLogger = new DuplicatingLogger( infoLoggers );
        this.warnLogger = new DuplicatingLogger( warnLoggers );
        this.errorLogger = new DuplicatingLogger( errorLoggers );
    }

    @Override
    public boolean isDebugEnabled()
    {
        for ( Log log : logs )
        {
            if ( log.isDebugEnabled() )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Logger debugLogger()
    {
        return this.debugLogger;
    }

    @Override
    public Logger infoLogger()
    {
        return this.infoLogger;
    }

    @Override
    public Logger warnLogger()
    {
        return this.warnLogger;
    }

    @Override
    public Logger errorLogger()
    {
        return this.errorLogger;
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        bulk( 0, new Log[logs.length], consumer );
    }

    private void bulk( final int logIdx, final Log[] bulkLogs, final Consumer<Log> finalConsumer )
    {
        if ( logIdx < logs.length )
        {
            Log log = logs[logIdx];
            log.bulk( new Consumer<Log>()
            {
                @Override
                public void accept( Log bulkLog )
                {
                    bulkLogs[logIdx] = bulkLog;
                    bulk( logIdx + 1, bulkLogs, finalConsumer );
                }
            } );
        } else
        {
            Log log = new DuplicatingLog( bulkLogs );
            finalConsumer.accept( log );
        }
    }
}
