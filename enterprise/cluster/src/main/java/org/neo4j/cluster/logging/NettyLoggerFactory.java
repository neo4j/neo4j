/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
