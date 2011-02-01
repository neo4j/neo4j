/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.osgi;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;
import org.neo4j.server.logging.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.log.LogService;

/**
 * Provides an OSGi LogService which pipes through to the Neo4j Logger.
 */
public class LogServiceBridge implements BundleActivator
{
    public static Logger log = Logger.getLogger( Logger.class );
    private LogServiceImpl logService;

    @Override
    public void start( BundleContext bundleContext ) throws Exception
    {
        logService = new LogServiceImpl( log );

        bundleContext.registerService( LogService.class.getName(), logService, null );
        log.info( "OSGi LogService Bridge started" );
    }

    @Override
    public void stop( BundleContext bundleContext ) throws Exception
    {
        log.info( "OSGi LogService Bridge stopped" );
    }

    private class LogServiceImpl implements LogService
    {
        private Logger log;

        public LogServiceImpl( Logger log )
        {
            this.log = log;
        }

        @Override
        public void log( int level, String message )
        {
            switch ( level )
            {
                case LOG_INFO:
                    log.info( message );
                    break;
                case LOG_DEBUG:
                    log.debug( message );
                    break;
                case LOG_ERROR:
                    log.error( message );
                    break;
                case LOG_WARNING:
                    log.warn( message );
                    break;
            }
        }

        @Override
        public void log( int level, String message, Throwable exception )
        {
            Priority neoLogPriority = null;
            switch ( level )
            {
                case LOG_INFO:
                    neoLogPriority = Level.INFO;
                    break;
                case LOG_DEBUG:
                    neoLogPriority = Level.DEBUG;
                    break;
                case LOG_ERROR:
                    neoLogPriority = Level.ERROR;
                    break;
                case LOG_WARNING:
                    neoLogPriority = Level.WARN;
                    break;
            }
            log.log(neoLogPriority, message, exception);
        }

        @Override
        public void log( ServiceReference sr, int level, String message )
        {
            log(level, sr.getClass().getName() + ": " + message);
        }

        @Override
        public void log( ServiceReference sr, int level, String message, Throwable exception )
        {
            log(level, sr.getClass().getName() + ": " + message, exception);
        }
    }
}
