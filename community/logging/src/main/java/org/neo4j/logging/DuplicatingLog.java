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

import org.neo4j.function.Consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A {@link Log} implementation that duplicates all messages to other Log instances
 */
public class DuplicatingLog extends AbstractLog
{
    private final CopyOnWriteArraySet<Log> logs;
    private final DuplicatingLogger debugLogger;
    private final DuplicatingLogger infoLogger;
    private final DuplicatingLogger warnLogger;
    private final DuplicatingLogger errorLogger;

    /**
     * @param logs A list of {@link Log} instances that messages should be duplicated to
     */
    public DuplicatingLog( Log... logs )
    {
        this( Arrays.asList( logs ) );
    }

    /**
     * @param logs A list of {@link Log} instances that messages should be duplicated to
     */
    public DuplicatingLog( List<Log> logs )
    {
        ArrayList<Logger> debugLoggers = new ArrayList<>( logs.size() );
        ArrayList<Logger> infoLoggers = new ArrayList<>( logs.size() );
        ArrayList<Logger> warnLoggers = new ArrayList<>( logs.size() );
        ArrayList<Logger> errorLoggers = new ArrayList<>( logs.size() );

        for ( Log log : logs )
        {
            debugLoggers.add( log.debugLogger() );
            infoLoggers.add( log.infoLogger() );
            warnLoggers.add( log.warnLogger() );
            errorLoggers.add( log.errorLogger() );
        }

        this.logs = new CopyOnWriteArraySet<>( logs );
        this.debugLogger = new DuplicatingLogger( debugLoggers );
        this.infoLogger = new DuplicatingLogger( infoLoggers );
        this.warnLogger = new DuplicatingLogger( warnLoggers );
        this.errorLogger = new DuplicatingLogger( errorLoggers );
    }

    /**
     * Remove a {@link Log} from the duplicating set
     *
     * @param log the Log to be removed
     * @return true if the log was found and removed
     */
    public boolean remove( Log log )
    {
        boolean removed = this.logs.remove( log );
        this.debugLogger.remove( log.debugLogger() );
        this.infoLogger.remove( log.infoLogger() );
        this.warnLogger.remove( log.warnLogger() );
        this.errorLogger.remove( log.errorLogger() );
        return removed;
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
        bulk( new LinkedList<>( logs ), new ArrayList<Log>( logs.size() ), consumer );
    }

    private static void bulk( final LinkedList<Log> remaining, final ArrayList<Log> bulkLogs, final Consumer<Log> finalConsumer )
    {
        if ( !remaining.isEmpty() )
        {
            Log log = remaining.pop();
            log.bulk( new Consumer<Log>()
            {
                @Override
                public void accept( Log bulkLog )
                {
                    bulkLogs.add( bulkLog );
                    bulk( remaining, bulkLogs, finalConsumer );
                }
            } );
        } else
        {
            Log log = new DuplicatingLog( bulkLogs );
            finalConsumer.accept( log );
        }
    }

    private static class DuplicatingLogger implements Logger
    {
        private final CopyOnWriteArraySet<Logger> loggers;

        public DuplicatingLogger( List<Logger> loggers )
        {
            this.loggers = new CopyOnWriteArraySet<>( loggers );
        }

        public boolean remove( Logger logger )
        {
            return this.loggers.remove( logger );
        }

        @Override
        public void log( String message )
        {
            for ( Logger logger : loggers )
            {
                logger.log( message );
            }
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            for ( Logger logger : loggers )
            {
                logger.log( message, throwable );
            }
        }

        @Override
        public void log( String format, Object... arguments )
        {
            for ( Logger logger : loggers )
            {
                logger.log( format, arguments );
            }
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            bulk( new LinkedList<>( loggers ), new ArrayList<Logger>( loggers.size() ), consumer );
        }

        private static void bulk( final LinkedList<Logger> remaining, final ArrayList<Logger> bulkLoggers, final Consumer<Logger> finalConsumer )
        {
            if ( !remaining.isEmpty() )
            {
                Logger logger = remaining.pop();
                logger.bulk( new Consumer<Logger>()
                {
                    @Override
                    public void accept( Logger bulkLogger )
                    {
                        bulkLoggers.add( bulkLogger );
                        bulk( remaining, bulkLoggers, finalConsumer );
                    }
                } );
            } else
            {
                Logger logger = new DuplicatingLogger( bulkLoggers );
                finalConsumer.accept( logger );
            }
        }
    }
}
