/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import io.netty.util.internal.logging.AbstractInternalLogger;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This class replaces Nettys regular logging system, injecting our own.
 */
public class Netty4LoggerFactory extends InternalLoggerFactory
{
    private LogProvider logProvider;

    public Netty4LoggerFactory( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    @Override
    public InternalLogger newInstance( String name )
    {
        final Log log = logProvider.getLog( name );
        return new AbstractInternalLogger( name )
        {
            @Override
            public boolean isTraceEnabled()
            {
                return false;
            }

            @Override
            public boolean isDebugEnabled()
            {
                return false;
            }

            @Override
            public boolean isInfoEnabled()
            {
                return false;
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
            public void debug( String s )
            {
                log.debug( s );
            }

            @Override
            public void debug( String s, Object o )
            {
                log.debug( s, o );
            }

            @Override
            public void debug( String s, Object o, Object o1 )
            {
                log.debug( s, o, o1 );
            }

            @Override
            public void debug( String s, Object... objects )
            {
                log.debug( s, objects );
            }

            @Override
            public void debug( String s, Throwable throwable )
            {
                log.debug( s, throwable );
            }

            @Override
            public void info( String s )
            {
                log.info( s );
            }

            @Override
            public void info( String s, Object o )
            {
                log.info( s, o );
            }

            @Override
            public void info( String s, Object o, Object o1 )
            {
                log.info( s, o, o1 );
            }

            @Override
            public void info( String s, Object... objects )
            {
                log.info( s, objects );
            }

            @Override
            public void info( String s, Throwable throwable )
            {
                log.info( s, throwable );
            }

            @Override
            public void warn( String s )
            {
                log.warn( s );
            }

            @Override
            public void warn( String s, Object o )
            {
                log.warn( s, o );
            }

            @Override
            public void warn( String s, Object... objects )
            {
                log.warn( s, objects );
            }

            @Override
            public void warn( String s, Object o, Object o1 )
            {
                log.warn( s, o, o1 );
            }

            @Override
            public void warn( String s, Throwable throwable )
            {
                log.warn( s, throwable );
            }

            @Override
            public void error( String s )
            {
                log.error( s );
            }

            @Override
            public void error( String s, Object o )
            {
                log.error( s, o );
            }

            @Override
            public void error( String s, Object o, Object o1 )
            {
                log.error( s, o, o1 );
            }

            @Override
            public void error( String s, Object... objects )
            {
                log.error( s, objects );
            }

            @Override
            public void error( String s, Throwable throwable )
            {
                log.error( s, throwable );
            }

            @Override
            public void trace( String s )
            {

            }

            @Override
            public void trace( String s, Object o )
            {

            }

            @Override
            public void trace( String s, Object o, Object o1 )
            {

            }

            @Override
            public void trace( String s, Object... objects )
            {

            }

            @Override
            public void trace( String s, Throwable throwable )
            {

            }

        };
    }
}
