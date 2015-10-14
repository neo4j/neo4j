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

import org.neo4j.concurrent.AsyncEvent;
import org.neo4j.logging.Logger;

/**
 * {@link AsyncEvent} representing one log statement issued from a {@link Logger}. Delegation to
 * an actual {@link Logger} takes place in {@link #process()}.
 */
abstract class AsyncLogEvent extends AsyncEvent
{
    private AsyncLogEvent()
    {
        // Instances are created by the static factory methods only
    }

    public static AsyncLogEvent logEvent( final Logger logger, final String message )
    {
        return new AsyncLogEvent()
        {
            @Override
            void process()
            {
                logger.log( message );
            }
        };
    }

    public static AsyncLogEvent logEvent( final Logger logger, final String message, final Throwable cause )
    {
        return new AsyncLogEvent()
        {
            @Override
            void process()
            {
                logger.log( message, cause );
            }
        };
    }

    public static AsyncLogEvent logEvent( final Logger logger, final String format, final Object[] arguments )
    {
        return new AsyncLogEvent()
        {
            @Override
            void process()
            {
                logger.log( format, arguments );
            }
        };
    }

    /**
     * Forwards this log event to an actual logger, the final step of a log event. This method
     * should be called by a thread dedicated for logging, one which can cope with latencies caused
     * by saturated I/O.
     */
    abstract void process();
}
