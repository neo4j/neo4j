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
package org.neo4j.logging;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
        List<Supplier<Logger>> debugLoggers = new ArrayList<>( logs.size() );
        List<Supplier<Logger>> infoLoggers = new ArrayList<>( logs.size() );
        List<Supplier<Logger>> warnLoggers = new ArrayList<>( logs.size() );
        List<Supplier<Logger>> errorLoggers = new ArrayList<>( logs.size() );

        for ( Log log : logs )
        {
            debugLoggers.add( log::debugLogger );
            infoLoggers.add( log::infoLogger );
            warnLoggers.add( log::warnLogger );
            errorLoggers.add( log::errorLogger );
        }

        this.logs = new CopyOnWriteArraySet<>( logs );
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

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return this.debugLogger;
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return this.infoLogger;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return this.warnLogger;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return this.errorLogger;
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        bulk( new ArrayDeque<>( logs ), new ArrayList<>( logs.size() ), consumer );
    }

    private static void bulk( final Deque<Log> remaining, final List<Log> bulkLogs, final Consumer<Log> finalConsumer )
    {
        if ( !remaining.isEmpty() )
        {
            Log log = remaining.pop();
            log.bulk( bulkLog ->
            {
                bulkLogs.add( bulkLog );
                bulk( remaining, bulkLogs, finalConsumer );
            } );
        }
        else
        {
            Log log = new DuplicatingLog( bulkLogs );
            finalConsumer.accept( log );
        }
    }

    private static class DuplicatingLogger implements Logger
    {
        private final CopyOnWriteArraySet<Supplier<Logger>> loggers;

        DuplicatingLogger( List<Supplier<Logger>> loggers )
        {
            this.loggers = new CopyOnWriteArraySet<>( loggers );
        }

        @Override
        public void log( @Nonnull String message )
        {
            for ( Supplier<Logger> logger : loggers )
            {
                logger.get().log( message );
            }
        }

        @Override
        public void log( @Nonnull String message, @Nonnull Throwable throwable )
        {
            for ( Supplier<Logger> logger : loggers )
            {
                logger.get().log( message, throwable );
            }
        }

        @Override
        public void log( @Nonnull String format, @Nullable Object... arguments )
        {
            for ( Supplier<Logger> logger : loggers )
            {
                logger.get().log( format, arguments );
            }
        }

        @Override
        public void bulk( @Nonnull Consumer<Logger> consumer )
        {
            bulk( new ArrayDeque<>( loggers ), new ArrayList<>( loggers.size() ), consumer );
        }

        private static void bulk( final Deque<Supplier<Logger>> remaining, final List<Supplier<Logger>> bulkLoggers,
                final Consumer<Logger> finalConsumer )
        {
            if ( !remaining.isEmpty() )
            {
                Logger logger = remaining.pop().get();
                logger.bulk( bulkLogger ->
                {
                    bulkLoggers.add( () -> bulkLogger );
                    bulk( remaining, bulkLoggers, finalConsumer );
                } );
            }
            else
            {
                Logger logger = new DuplicatingLogger( bulkLoggers );
                finalConsumer.accept( logger );
            }
        }
    }
}
