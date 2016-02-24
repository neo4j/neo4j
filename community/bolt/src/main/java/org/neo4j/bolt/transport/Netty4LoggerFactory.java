/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.transport;

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
                return log.isDebugEnabled();
            }

            @Override
            public boolean isInfoEnabled()
            {
                // No way to tell log level with better granularity yet, and INFO
                // logging for Netty component is most likely DEBUG anyway
                return log.isDebugEnabled();
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
                log.debug( asNeoTemplate(s), o );
            }

            @Override
            public void debug( String s, Object o, Object o1 )
            {
                log.debug( asNeoTemplate(s), o, o1 );
            }

            @Override
            public void debug( String s, Object... objects )
            {
                log.debug( asNeoTemplate(s), objects );
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
                log.info( asNeoTemplate(s), o );
            }

            @Override
            public void info( String s, Object o, Object o1 )
            {
                log.info( asNeoTemplate(s), o, o1 );
            }

            @Override
            public void info( String s, Object... objects )
            {
                log.info( asNeoTemplate(s), objects );
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
                log.warn( asNeoTemplate(s), o );
            }

            @Override
            public void warn( String s, Object... objects )
            {
                log.warn( asNeoTemplate(s), objects );
            }

            @Override
            public void warn( String s, Object o, Object o1 )
            {
                log.warn( asNeoTemplate(s), o, o1 );
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
                log.error( asNeoTemplate(s), o );
            }

            @Override
            public void error( String s, Object o, Object o1 )
            {
                log.error( asNeoTemplate(s), o, o1 );
            }

            @Override
            public void error( String s, Object... objects )
            {
                log.error( asNeoTemplate(s), objects );
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

            private String asNeoTemplate( String nettyLogTemplate )
            {
                // Netty uses MessageFormat as placeholders, we use String.format()
                // Sidenote: MessageFormat is the right tool for this job, it handles
                // pluralization, i18n etc., we should change at some point.
                return nettyLogTemplate.replaceAll( "\\{\\}", "%s" );
            }

        };
    }
}
