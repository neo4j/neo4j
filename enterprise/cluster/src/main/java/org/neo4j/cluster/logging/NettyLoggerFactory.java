/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.logging;


import io.netty.util.internal.logging.AbstractInternalLogger;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class NettyLoggerFactory
    extends InternalLoggerFactory
{
    private Logging logging;

    public NettyLoggerFactory( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public InternalLogger newInstance( String name )
    {
        StringLogger logger;
        try
        {
            logger = logging.getMessagesLog( getClass().getClassLoader().loadClass( name ) );
        }
        catch ( ClassNotFoundException e )
        {
            logger = logging.getMessagesLog( getClass() );
        }

        final StringLogger finalLogger = logger;
        return new AbstractInternalLogger("Neo4jLogger")
        {
            @Override
            public void trace( String msg )
            {

            }

            @Override
            public void trace( String format, Object arg )
            {

            }

            @Override
            public void trace( String format, Object argA, Object argB )
            {

            }

            @Override
            public void trace( String format, Object... arguments )
            {

            }

            @Override
            public void trace( String msg, Throwable t )
            {

            }

            @Override
            public boolean isTraceEnabled()
            {
                return false;
            }

            @Override
            public boolean isDebugEnabled()
            {
                return finalLogger.isDebugEnabled();
            }

            @Override
            public boolean isInfoEnabled()
            {
                return true;
            }

            @Override
            public boolean isWarnEnabled()
            {
                return true;
            }

            @Override
            public boolean isErrorEnabled()
            {
                return true;
            }

            @Override
            public boolean isEnabled( InternalLogLevel level )
            {
                return true;
            }

            @Override
            public void debug( String msg )
            {
                finalLogger.debug( msg );
            }

            @Override
            public void debug( String format, Object arg )
            {
                finalLogger.debug( String.format( format, arg ) );
            }

            @Override
            public void debug( String format, Object argA, Object argB )
            {
                finalLogger.debug( String.format( format, argA, argB ) );
            }

            @Override
            public void debug( String format, Object... arguments )
            {
                finalLogger.debug( String.format( format, arguments ) );
            }

            @Override
            public void debug( String msg, Throwable cause )
            {
                finalLogger.debug( msg, cause );
            }

            @Override
            public void info( String msg )
            {
                finalLogger.info( msg );
            }

            @Override
            public void info( String format, Object arg )
            {
                finalLogger.info( String.format( format, arg ) );
            }

            @Override
            public void info( String format, Object argA, Object argB )
            {
                finalLogger.info( String.format( format, argA, argB ) );
            }

            @Override
            public void info( String format, Object... arguments )
            {
                finalLogger.info( String.format( format, arguments ) );
            }

            @Override
            public void info( String msg, Throwable cause )
            {
                finalLogger.info( msg, cause );
            }

            @Override
            public void warn( String msg )
            {
                finalLogger.warn( msg );
            }

            @Override
            public void warn( String format, Object arg )
            {
                finalLogger.warn( String.format( format, arg ) );
            }

            @Override
            public void warn( String format, Object... arguments )
            {
                finalLogger.warn( String.format( format, arguments ) );
            }

            @Override
            public void warn( String format, Object argA, Object argB )
            {
                finalLogger.warn( String.format( format, argA, argB ) );
            }

            @Override
            public void warn( String msg, Throwable cause )
            {
                finalLogger.warn( msg, cause );
            }

            @Override
            public void error( String msg )
            {
                finalLogger.error( msg );
            }

            @Override
            public void error( String format, Object arg )
            {
                finalLogger.error( String.format( format, arg ) );
            }

            @Override
            public void error( String format, Object argA, Object argB )
            {
                finalLogger.error( String.format( format, argA, argB ) );
            }

            @Override
            public void error( String format, Object... arguments )
            {
                finalLogger.error( String.format( format, arguments ) );
            }

            @Override
            public void error( String msg, Throwable cause )
            {
                finalLogger.error( msg, cause );
            }
        };
    }
}
