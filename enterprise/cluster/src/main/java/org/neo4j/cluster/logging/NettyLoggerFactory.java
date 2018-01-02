/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.jboss.netty.logging.AbstractInternalLogger;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Adapter which send Netty logging messages to our internal log.
 */
public class NettyLoggerFactory
    extends InternalLoggerFactory
{
    private LogProvider logProvider;

    public NettyLoggerFactory( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    @Override
    public InternalLogger newInstance( String name )
    {
        final Log log = logProvider.getLog( name );
        return new AbstractInternalLogger()
        {
            @Override
            public boolean isDebugEnabled()
            {
                return log.isDebugEnabled();
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
                return level != InternalLogLevel.DEBUG || isDebugEnabled();
            }

            @Override
            public void debug( String msg )
            {
                log.debug( msg );
            }

            @Override
            public void debug( String msg, Throwable cause )
            {
                log.debug( msg, cause );
            }

            @Override
            public void info( String msg )
            {
                log.info( msg );
            }

            @Override
            public void info( String msg, Throwable cause )
            {
                log.info( msg, cause );
            }

            @Override
            public void warn( String msg )
            {
                log.warn( msg );
            }

            @Override
            public void warn( String msg, Throwable cause )
            {
                log.warn( msg, cause );
            }

            @Override
            public void error( String msg )
            {
                log.error( msg );
            }

            @Override
            public void error( String msg, Throwable cause )
            {
                log.error( msg, cause );
            }
        };
    }
}
