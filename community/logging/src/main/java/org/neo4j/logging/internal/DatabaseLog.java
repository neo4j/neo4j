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
package org.neo4j.logging.internal;

import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogger;

public class DatabaseLog extends AbstractLog
{
    private final DatabaseLogContext logContext;
    private final Log delegate;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    DatabaseLog( DatabaseLogContext logContext, Log delegate )
    {
        this.logContext = logContext;
        this.delegate = delegate;

        // use logger suppliers because the log delegate is allowed to return different loggers based on the configured log level
        this.debugLogger = new DatabaseLogger( logContext, delegate::debugLogger );
        this.infoLogger = new DatabaseLogger( logContext, delegate::infoLogger );
        this.warnLogger = new DatabaseLogger( logContext, delegate::warnLogger );
        this.errorLogger = new DatabaseLogger( logContext, delegate::errorLogger );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return delegate.isDebugEnabled();
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        // check if debug is enabled to avoid string concatenation to create a prefix
        return isDebugEnabled() ? debugLogger : NullLogger.getInstance();
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return infoLogger;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return warnLogger;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return errorLogger;
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        delegate.bulk( log -> consumer.accept( new DatabaseLog( logContext, log ) ) );
    }
}
