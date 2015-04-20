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
package org.neo4j.kernel.logging;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.String.format;

public class BufferingConsoleLogger extends ConsoleLogger
{
    private static enum Level
    {
        LOG,
        WARN,
        ERROR;
    }

    private static class LogMessage
    {
        private final Level level;
        private final String message;
        private final Throwable throwable;

        public LogMessage( Level level, String message, Throwable throwable )
        {
            this.level = level;
            this.message = message;
            this.throwable = throwable;
        }
    }

    private final List<LogMessage> buffer = new ArrayList<LogMessage>();

    public BufferingConsoleLogger()
    {
        super( StringLogger.DEV_NULL );
    }

    @Override
    public void log( String message )
    {
        buffer.add( new LogMessage( Level.LOG, message, null ) );
    }

    @Override
    public void warn( String message, Throwable warning )
    {
        buffer.add( new LogMessage( Level.WARN, message, warning ) );
    }

    @Override
    public void error( String message, Throwable error )
    {
        buffer.add( new LogMessage( Level.ERROR, message, error ) );
    }

    public void replayInto( ConsoleLogger target )
    {
        for ( LogMessage item : buffer )
        {
            switch ( item.level )
            {
            case LOG:
                target.log( item.message );
                break;
            case WARN:
                if ( item.throwable != null )
                {
                    target.warn( item.message, item.throwable );
                }
                else
                {
                    target.warn( item.message );
                }
                break;
            case ERROR:
                if ( item.throwable != null )
                {
                    target.error( item.message, item.throwable );
                }
                else
                {
                    target.error( item.message );
                }
                break;
            default:
                throw new IllegalArgumentException( "Unknown level " + item.level );
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( LogMessage message : buffer )
        {
            builder
                    .append( message.message )
                    .append( message.throwable != null ? message.throwable.getMessage() : "" )
                    .append( format( "%n" ) );
        }
        return builder.toString();
    }
}
