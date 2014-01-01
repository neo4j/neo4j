/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.logging;

import java.util.logging.Level;

public class Logger
{
    java.util.logging.Logger logger;

    public static Logger getLogger( Class<?> clazz )
    {
        return new Logger( clazz );
    }

    public static Logger getLogger( String logger )
    {
        return new Logger( logger );
    }

    public Logger( Class<?> clazz )
    {
        this( clazz.getCanonicalName() );
    }

    public Logger( String str )
    {
        logger = java.util.logging.Logger.getLogger( str );
    }

    public void log( Level priority, String message, Throwable throwable )
    {
        logger.log( priority, message, throwable );
    }

    public void log( Level level, String message, Object... parameters )
    {
        // guard the call to string.format
        if ( logger.isLoggable( level ) )
        {
            final String logMessage = String.format( message, parameters );

            logger.log( level, logMessage );
        }
    }

    public void fatal( String message, Object... parameters )
    {
        log( Level.SEVERE, message, parameters );
    }

    public void error( String message, Object... parameters )
    {
        log( Level.SEVERE, message, parameters );
    }

    public void error( Throwable e )
    {
        log( Level.SEVERE, "", e );
    }

    public void warn( Throwable e )
    {
        log( Level.WARNING, "", e );
    }

    public void warn( String message, Object... parameters )
    {
        log( Level.WARNING, message, parameters );
    }

    public void info( String message, Object... parameters )
    {
        log( Level.INFO, message, parameters );
    }

    public void debug( String message, Object... parameters )
    {
        log( Level.FINE, message, parameters );
    }

    public void trace( String message, Object... parameters )
    {
        log( Level.FINEST, message, parameters );
    }
}
