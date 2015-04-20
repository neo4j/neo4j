/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ext.monitorlogging;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class MonitorLoggingExtension implements Lifecycle
{
    private final Properties props;
    private final Logging logging;
    private final Monitors monitors;

    public MonitorLoggingExtension( Properties props, Logging logging, Monitors monitors )
    {
        this.props = props;
        this.logging = logging;
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

        final Map<Class<?>, LogLevel> clazzez = new HashMap<>( classes.size() );
        for ( Map.Entry<Object, Object> entry : classes )
        {
            String className = (String) entry.getKey();
            String logLevel = (String) entry.getValue();
            try
            {
                clazzez.put( getClass().getClassLoader().loadClass( className ), LogLevel.valueOf( logLevel.toUpperCase() ));
            }
            catch ( ClassNotFoundException ex )
            {
                logging.getMessagesLog( getClass() )
                        .warn( "When trying to add a logging monitor, not able to load class " + className, ex );
            } catch ( IllegalArgumentException ex) {
                logging.getMessagesLog( getClass() )
                        .warn( "When trying to add a logging monitor for " + className + " not able to understand the log level, got " + logLevel, ex );
            }
        }

        if ( clazzez.isEmpty() )
        {
            return;
        }

        LoggingListener listener = new LoggingListener( logging, clazzez );
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
