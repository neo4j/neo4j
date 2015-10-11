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

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.function.Consumer;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

/**
 * Wraps a {@link Log}, making its logging asynchronous by letting log statements be treated as events fed into a
 * {@link AsyncEventSender} to be processed later instead of in the thread making the log statement call.
 */
class AsyncLog extends AbstractLog
{
    private final Log actual;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    AsyncLog( AsyncEventSender<AsyncLogEvent> sender, final Log actual )
    {
        this.debugLogger = new AsyncLogger( sender )
        {
            @Override
            protected Logger logger()
            {
                return actual.debugLogger();
            }

            @Override
            protected boolean isEnabled()
            {
                return actual.isDebugEnabled();
            }
        };
        this.infoLogger = new AsyncLogger( sender )
        {
            @Override
            protected Logger logger()
            {
                return actual.infoLogger();
            }

            @Override
            protected boolean isEnabled()
            {
                return true;
            }
        };
        this.warnLogger = new AsyncLogger( sender )
        {
            @Override
            protected Logger logger()
            {
                return actual.warnLogger();
            }

            @Override
            protected boolean isEnabled()
            {
                return true;
            }
        };
        this.errorLogger = new AsyncLogger( sender )
        {
            @Override
            protected Logger logger()
            {
                return actual.errorLogger();
            }

            @Override
            protected boolean isEnabled()
            {
                return true;
            }
        };
        this.actual = actual;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return actual.isDebugEnabled();
    }

    @Override
    public Logger debugLogger()
    {
        return debugLogger;
    }

    @Override
    public Logger infoLogger()
    {
        return infoLogger;
    }

    @Override
    public Logger warnLogger()
    {
        return warnLogger;
    }

    @Override
    public Logger errorLogger()
    {
        return errorLogger;
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        consumer.accept( this );
    }
}
