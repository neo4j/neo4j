/**
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
package org.neo4j.logging;

import org.neo4j.function.Consumer;

import java.util.List;

/**
 * A {@link Logger} implementation that duplicates all messages to other Logger instances
 */
public class DuplicatingLogger implements Logger
{
    private final Logger[] loggers;

    /**
     * @param loggers A list of {@link Logger} instances that messages should be duplicated to
     */
    public DuplicatingLogger( List<Logger> loggers )
    {
        this( loggers.toArray( new Logger[loggers.size()] ) );
    }

    /**
     * @param loggers A list of {@link Logger} instances that messages should be duplicated to
     */
    public DuplicatingLogger( Logger... loggers )
    {
        this.loggers = loggers;
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
        bulk( 0, new Logger[loggers.length], consumer );
    }

    private void bulk( final int loggerIdx, final Logger[] bulkLoggers, final Consumer<Logger> finalConsumer )
    {
        if ( loggerIdx < loggers.length )
        {
            Logger logger = loggers[loggerIdx];
            logger.bulk( new Consumer<Logger>()
            {
                @Override
                public void accept( Logger bulkLogger )
                {
                    bulkLoggers[loggerIdx] = bulkLogger;
                    bulk( loggerIdx + 1, bulkLoggers, finalConsumer );
                }
            } );
        } else
        {
            Logger logger = new DuplicatingLogger( bulkLoggers );
            finalConsumer.accept( logger );
        }
    }
}
