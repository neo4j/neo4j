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
package org.neo4j.server.logging;

import org.eclipse.jetty.util.log.AbstractLogger;
import org.eclipse.jetty.util.log.Logger;

import java.util.IllegalFormatException;
import java.util.regex.Pattern;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.NeoServer;
import org.neo4j.server.web.WebServer;

/**
 * Simple wrapper over the neo4j native logger class for getting jetty to use
 * our logging framework. See <a
 * href="http://docs.codehaus.org/display/JETTY/Debugging">the Jetty Debug
 * page</a> for more info.
 *
 * @author Chris Gioran
 */
public class JettyLoggerAdapter extends AbstractLogger
{
    private static final String SYSTEM = "SYSTEM";
    private static final Pattern percentageRegex = Pattern.compile( "%" );
    private static final Pattern slfPlaceholderRegex = Pattern.compile( "\\{\\}" );

    private static volatile Logging GLOBAL_LOGGING = null;

    private final Logging logging;
    private final StringLogger logger;

    /**
     * This is crap, but jetty loads this class through reflection and requires a no-arg constructor to accept it.
     * As such, I can't think of a way to do this without a global static field. The alternative is to configure
     * slf4j to go via logback (which is globally configured), but thats just a very cumbersome way to do what we are
     * doing here through xml files.
     */
    public static void setGlobalLogging( Logging logging )
    {
        GLOBAL_LOGGING = logging;
    }

    public JettyLoggerAdapter()
    {
        this( GLOBAL_LOGGING );
    }

    public JettyLoggerAdapter( Logging logging )
    {
        this.logging = logging;
        this.logger = logging.getMessagesLog( NeoServer.class );
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
    public void debug( String msg, Throwable cause )
    {
        try
        {
            logger.debug( wrapNull( msg ), cause );
        }
        catch ( IllegalFormatException e )
        {
            logger.debug( safeFormat( msg, cause ) );
        }
    }

    @Override
    public void debug( String msg, Object... args )
    {
        try
        {
            logger.debug( format( wrapNull( msg ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.debug( safeFormat( msg, args ) );
        }
    }

    @Override
    public Logger newLogger( String name )
    {
        return new JettyLoggerAdapter( logging, logging.getMessagesLog( WebServer.class ) );
    }

    @Override
    public void info( String msg, Object... args )
    {
        try
        {
            logger.info( format( wrapNull( msg ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.info( safeFormat( msg, args ) );
        }
    }

    @Override
    public void info( Throwable cause )
    {
        logger.debug( cause.getMessage(), cause );
    }

    @Override
    public void info( String msg, Throwable cause )
    {
        logger.debug( msg, cause );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return logger.isDebugEnabled();
    }

    @Override
    public void setDebugEnabled( boolean debugEnabled )
    {
    }

    @Override
    public void warn( Throwable cause )
    {
        logger.warn( cause.getMessage(), cause );
    }

    @Override
    public void warn( String msg, Throwable cause )
    {
        logger.warn( wrapNull( cause ) );
    }

    @Override
    public void warn( String msg, Object... args )
    {
        try
        {
            logger.warn( format( wrapNull( msg ), args ) );
        }
        catch ( IllegalFormatException e )
        {
            logger.warn( safeFormat( msg, args ) );
        }
    }

    @Override
    public void ignore( Throwable cause )
    {
        logger.debug( cause.getMessage(), cause );
    }

    @Override
    public String getName()
    {
        return SYSTEM;
    }

    static String safeFormat( String format, Object... args )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "Failed to format message: " );
        builder.append( armored( format ) );
        if ( null != args )
        {
            for ( int i = 0; i < args.length; i++ )
            {
                appendArg( builder, i + 1, args[i] );
            }
        }
        return builder.toString();
    }

    private static String format( String format, Object... args )
    {
        // Jetty messages use SLF4J message patterns, using '{}' as placeholders, so we convert that here
        return String.format( format.replace( "{}", "%s" ), args );
    }

    private static String armored( Object msg )
    {
        return percentageRegex.matcher( wrapNull( msg ) ).replaceAll( "?" );
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
}