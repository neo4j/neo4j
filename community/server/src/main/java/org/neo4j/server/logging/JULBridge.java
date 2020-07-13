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
package org.neo4j.server.logging;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class JULBridge extends Handler
{
    private static final String UNKNOWN_LOGGER_NAME = "unknown";

    private final LogProvider logProvider;
    private final ConcurrentMap<String, Log> logs = new ConcurrentHashMap<>();

    protected JULBridge( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public static void resetJUL()
    {
        LogManager.getLogManager().reset();
    }

    public static void forwardTo( LogProvider logProvider )
    {
        rootJULLogger().addHandler( new JULBridge( logProvider ) );
    }

    private static java.util.logging.Logger rootJULLogger()
    {
        return LogManager.getLogManager().getLogger( "" );
    }

    @Override
    public void publish( LogRecord record )
    {
        if ( record == null )
        {
            return;
        }

        String message = getMessage( record );
        if ( message == null )
        {
            return;
        }

        String context = record.getLoggerName();
        Log log = getLog( ( context != null ) ? context : UNKNOWN_LOGGER_NAME );
        log( log, record.getLevel().intValue(), message, record.getThrown() );
    }

    private void log( Log log, int level, String message, Throwable throwable )
    {
        if ( level <= Level.FINE.intValue() )
        {
            if ( throwable == null )
            {
                log.debug( message );
            }
            else
            {
                log.debug( message, throwable );
            }
        }
        else if ( level <= Level.INFO.intValue() )
        {
            if ( throwable == null )
            {
                log.info( message );
            }
            else
            {
                log.info( message, throwable );
            }
        }
        else if ( level <= Level.WARNING.intValue() )
        {
            if ( throwable == null )
            {
                log.warn( message );
            }
            else
            {
                log.warn( message, throwable );
            }
        }
        else
        {
            if ( throwable == null )
            {
                log.error( message );
            }
            else
            {
                log.error( message, throwable );
            }
        }
    }

    private Log getLog( String name )
    {
        Log log = logs.get( name );
        if ( log == null )
        {
            Log newLog = logProvider.getLog( name );
            log = logs.putIfAbsent( name, newLog );
            if ( log == null )
            {
                log = newLog;
            }
        }
        return log;
    }

    private String getMessage( LogRecord record )
    {
        String message = record.getMessage();
        if ( message == null )
        {
            return null;
        }

        ResourceBundle bundle = record.getResourceBundle();
        if ( bundle != null )
        {
            try
            {
                message = bundle.getString( message );
            }
            catch ( MissingResourceException e )
            {
                // leave message as it was
            }
        }

        Object[] params = record.getParameters();
        if ( params != null && params.length > 0 )
        {
            message = MessageFormat.format( message, params );
        }
        return message;
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void close() throws SecurityException
    {
    }
}
