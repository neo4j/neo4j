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
package org.neo4j.ext.monitorlogging;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.kernel.monitoring.Monitors;

public class MonitorLoggingExtension implements Lifecycle
{
    private final Properties props;
    private final LogService logService;
    private final Log log;
    private final Monitors monitors;

    public MonitorLoggingExtension( Properties props, LogService logService, Monitors monitors )
    {
        this.props = props;
        this.logService = logService;
        this.log = logService.getInternalLog( getClass() );
        this.monitors = monitors;
    }

    @Override
    public void init() throws Throwable
    {
        Set<Map.Entry<Object, Object>> classes = props.entrySet();
        if ( classes.isEmpty() )
        {
            return;
        }

        final Map<Class<?>, Logger> clazzez = new HashMap<>( classes.size() );
        for ( Map.Entry<Object, Object> entry : classes )
        {
            String className = (String) entry.getKey();
            String logLevel = (String) entry.getValue();

            Class clazz;
            try
            {
                clazz = getClass().getClassLoader().loadClass( className );
            } catch ( ClassNotFoundException ex )
            {
                log.warn( "When trying to add a logging monitor, not able to load class " + className, ex );
                continue;
            }

            Log classLog = logService.getInternalLog( clazz );
            Logger logger;
            switch ( logLevel )
            {
                case "DEBUG":
                    logger = classLog.debugLogger();
                    break;
                case "INFO":
                    logger = classLog.infoLogger();
                    break;
                case "WARN":
                    logger = classLog.warnLogger();
                    break;
                case "ERROR":
                    logger = classLog.errorLogger();
                    break;
                default:
                    log.warn( "When trying to add a logging monitor for %s not able to understand the log level, got %s", className, logLevel );
                    continue;
            }

            clazzez.put( clazz, logger );
        }

        if ( clazzez.isEmpty() )
        {
            return;
        }

        LoggingListener listener = new LoggingListener( clazzez );
        monitors.addMonitorListener( listener, listener.predicate );
    }

    @Override
    public void start() throws Throwable
    {

    }

    @Override
    public void stop() throws Throwable
    {

    }

    @Override
    public void shutdown() throws Throwable
    {

    }
}
