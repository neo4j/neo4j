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
package org.neo4j.server.logging;

import java.util.IllegalFormatException;

import org.eclipse.jetty.util.log.Logger;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static java.lang.String.format;

/**
 * Simple wrapper over the neo4j native logger class for getting jetty to use
 * our logging framework. See <a
 * href="http://docs.codehaus.org/display/JETTY/Debugging">the Jetty Debug
 * page</a> for more info.
 *
 * @author Chris Gioran
 */
public class JettyLoggerAdapter implements Logger
{
    private static final String SYSTEM = "SYSTEM";
    
    private final Logging logging;
    private final StringLogger logger;

    public JettyLoggerAdapter( Logging logging )
    {
        this.logging = logging;
        this.logger = StringLogger.SYSTEM;
    }

    private JettyLoggerAdapter( Logging logging, StringLogger logger )
    {
        this.logging = logging;
        this.logger = logger;
    }

    @Override
    public void debug( Throwable arg1 )
    {
        logger.debug( arg1.getMessage(), arg1 );
    }
    
    @Override
    public void debug( String arg0, Throwable arg1 )
    {
        try
        {
            logger.debug( wrapNull( arg0 ), arg1 );
        }
        catch ( IllegalFormatException e )
        {
            logger.debug( safeFormat( arg0, arg1 ) );
        }
    }

    @Override
    public void debug( String arg0, Object... args )
    {
        try
        {
            logger.debug( format( wrapNull( arg0 ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.debug( safeFormat( arg0, args ) );
        }
    }

    @Override
    public void debug( String s, long l )
    {
        logger.debug(format(s, l));
    }

    @Override
    public Logger getLogger( String arg0 )
    {
        Class<?> cls = null;
        try
        {
            cls = Class.forName( arg0 );
        }
        catch ( ClassNotFoundException e )
        {
            cls = JettyLoggerAdapter.class;
        }

        return new JettyLoggerAdapter( logging, logging.getMessagesLog( cls ) );
    }

    @Override
    public void info( String arg0, Object... args )
    {
        try
        {
            logger.info( format( wrapNull( arg0 ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.info( safeFormat( arg0, args ) );
        }
    }

    @Override
    public void info( Throwable arg1 )
    {
        logger.debug( arg1.getMessage(), arg1 );
    }

    @Override
    public void info( String arg0, Throwable arg1 )
    {
        logger.debug( arg0, arg1 );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return true;
    }

    @Override
    public void setDebugEnabled( boolean arg0 )
    {
    }

    @Override
    public void warn( Throwable arg1 )
    {
        logger.debug( arg1.getMessage(), arg1 );
    }
    
    @Override
    public void warn( String arg0, Throwable arg1 )
    {
        // no need to catch IllegalFormatException as the delegate will use an empty message
        logger.warn( wrapNull( arg1 ) );
    }

    @Override
    public void warn( String arg0, Object... args )
    {
        try
        {
            logger.warn( format( wrapNull( arg0 ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.warn( safeFormat( arg0, args ) );
        }
    }

    static String safeFormat( String arg0, Object... args )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "Failed to format message: " );
        builder.append( armored( arg0 ) );
        if ( null != args )
        {
            for ( int i = 0; i < args.length; i++ )
            {
                appendArg( builder, i + 1, args[i] );
            }
        }
        return builder.toString();
    }

    private static String armored( Object arg0 )
    {
        return wrapNull( arg0 ).replaceAll( "%", "?" );
    }

    private static void appendArg( StringBuilder builder, int argNum, Object arg )
    {
        builder.append( " arg" );
        builder.append( argNum );
        builder.append( ": " );
        builder.append( armored( arg ) );
    }

    private static String wrapNull( Object arg0 )
    {
        return null == arg0 ? "null" : arg0.toString();
    }
    
    @Override
    public void ignore( Throwable arg1 )
    {
        logger.debug( arg1.getMessage(), arg1 );
    }

    @Override
    public String getName() {
        return SYSTEM;
    }
}
