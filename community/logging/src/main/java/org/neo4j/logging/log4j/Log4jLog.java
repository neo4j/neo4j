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
package org.neo4j.logging.log4j;

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;

/**
 * A {@link Log} implementation that uses the Log4j configuration the logger is connected to.
 */
public class Log4jLog extends AbstractLog
{
    final org.apache.logging.log4j.Logger logger;

    /**
     * Package-private specifically to not leak Logger outside logging module.
     * Should not be used outside of the logging module - {@link Log4jLogProvider#getLog} should be used instead.
     */
    Log4jLog( org.apache.logging.log4j.Logger logger )
    {
        this.logger = logger;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug( @Nonnull String message )
    {
        logger.debug( message );
    }

    @Override
    public void debug( @Nonnull String message, @Nonnull Throwable throwable )
    {
        logger.debug( message, throwable );
    }

    @Override
    public void debug( @Nonnull String format, @Nullable Object... arguments )
    {
        logger.printf( org.apache.logging.log4j.Level.DEBUG, format, arguments );
    }

    @Override
    public void info( Neo4jLogMessage message )
    {
        logger.info( message );
    }

    @Override
    public void info( @Nonnull String message )
    {
        logger.info( message );
    }

    @Override
    public void info( @Nonnull String message, @Nonnull Throwable throwable )
    {
        logger.info( message, throwable );
    }

    @Override
    public void info( @Nonnull String format, @Nullable Object... arguments )
    {
        logger.printf( org.apache.logging.log4j.Level.INFO, format, arguments );
    }

    @Override
    public void warn( @Nonnull String message )
    {
        logger.warn( message );
    }

    @Override
    public void warn( @Nonnull String message, @Nonnull Throwable throwable )
    {
        logger.warn( message, throwable );
    }

    @Override
    public void warn( @Nonnull String format, @Nullable Object... arguments )
    {
        logger.printf( org.apache.logging.log4j.Level.WARN, format, arguments );
    }

    @Override
    public void error( Neo4jLogMessage message )
    {
        logger.error( message );
    }

    @Override
    public void error( @Nonnull String message )
    {
        logger.error( message );
    }

    @Override
    public void error( Neo4jLogMessage message, Throwable throwable )
    {
        logger.error( message, throwable );
    }

    @Override
    public void error( @Nonnull String message, @Nonnull Throwable throwable )
    {
        logger.error( message, throwable );
    }

    @Override
    public void error( @Nonnull String format, @Nullable Object... arguments )
    {
        logger.printf( org.apache.logging.log4j.Level.ERROR, format, arguments );
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        consumer.accept(this);
    }
}
