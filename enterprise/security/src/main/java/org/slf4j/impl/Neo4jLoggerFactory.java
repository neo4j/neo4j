/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.NOPLogger;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class Neo4jLoggerFactory implements ILoggerFactory
{
    private final AtomicReference<LogProvider> logProvider = new AtomicReference<>();

    @Override
    public Logger getLogger( String name )
    {
        if ( logProvider.get() != null )
        {
            Log log = logProvider.get().getLog( name );
            return new Neo4jLogger( name, log );
        }
        return NOPLogger.NOP_LOGGER;
    }

    public void setNeo4jLogProvider( LogProvider logProvider )
    {
        this.logProvider.set( logProvider );
    }

    private class Neo4jLogger extends MarkerIgnoringBase
    {
        private final String name;
        private final Log log;

        public Neo4jLogger( String name, Log log )
        {
            this.name = name;
            this.log = log;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public boolean isTraceEnabled()
        {
            return false;
        }

        @Override
        public void trace( String msg )
        {
        }

        @Override
        public void trace( String format, Object arg )
        {
        }

        @Override
        public void trace( String format, Object arg1, Object arg2 )
        {
        }

        @Override
        public void trace( String format, Object[] argArray )
        {
        }

        @Override
        public void trace( String msg, Throwable t )
        {
        }

        @Override
        public boolean isDebugEnabled()
        {
            if ( log != null )
            {
                return log.isDebugEnabled();
            }
            return false;
        }

        @Override
        public void debug( String msg )
        {
            if ( log != null )
            {
                log.debug( msg );
            }
        }

        @Override
        public void debug( String format, Object arg )
        {
            if ( log != null )
            {
                log.debug( format, arg );
            }
        }

        @Override
        public void debug( String format, Object arg1, Object arg2 )
        {
            if ( log != null )
            {
                log.debug( format, arg1, arg2 );
            }
        }

        @Override
        public void debug( String format, Object[] argArray )
        {
            if ( log != null )
            {
                log.debug( format, argArray );
            }
        }

        @Override
        public void debug( String msg, Throwable t )
        {
            if ( log != null )
            {
                log.debug( msg, t );
            }
        }

        @Override
        public boolean isInfoEnabled()
        {
            if ( log != null )
            {
                return true;
            }
            return false;
        }

        @Override
        public void info( String msg )
        {
            if ( log != null )
            {
                log.info( msg );
            }
        }

        @Override
        public void info( String format, Object arg )
        {
            if ( log != null )
            {
                log.info( format, arg );
            }
        }

        @Override
        public void info( String format, Object arg1, Object arg2 )
        {
            if ( log != null )
            {
                log.info( format, arg1, arg2 );
            }
        }

        @Override
        public void info( String format, Object[] argArray )
        {
            if ( log != null )
            {
                log.info( format, argArray );
            }
        }

        @Override
        public void info( String msg, Throwable t )
        {
            if ( log != null )
            {
                log.info( msg, t );
            }
        }

        @Override
        public boolean isWarnEnabled()
        {
            if ( log != null )
            {
                return true;
            }
            return false;
        }

        @Override
        public void warn( String msg )
        {
            if ( log != null )
            {
                log.warn( msg );
            }
        }

        @Override
        public void warn( String format, Object arg )
        {
            if ( log != null )
            {
                log.warn( format, arg );
            }
        }

        @Override
        public void warn( String format, Object arg1, Object arg2 )
        {
            if ( log != null )
            {
                log.warn( format, arg1, arg2 );
            }
        }

        @Override
        public void warn( String format, Object[] argArray )
        {
            if ( log != null )
            {
                log.warn( format, argArray );
            }
        }

        @Override
        public void warn( String msg, Throwable t )
        {
            if ( log != null )
            {
                log.warn( msg, t );
            }
        }

        @Override
        public boolean isErrorEnabled()
        {
            if ( log != null )
            {
                return true;
            }
            return false;
        }

        @Override
        public void error( String msg )
        {
            if ( log != null )
            {
                log.error( msg );
            }
        }

        @Override
        public void error( String format, Object arg )
        {
            if ( log != null )
            {
                log.error( format, arg );
            }
        }

        @Override
        public void error( String format, Object arg1, Object arg2 )
        {
            if ( log != null )
            {
                log.error( format, arg1, arg2 );
            }
        }

        @Override
        public void error( String format, Object[] argArray )
        {
            if ( log != null )
            {
                log.error( format, argArray );
            }
        }

        @Override
        public void error( String msg, Throwable t )
        {
            if ( log != null )
            {
                log.error( msg, t );
            }
        }
    };
}
