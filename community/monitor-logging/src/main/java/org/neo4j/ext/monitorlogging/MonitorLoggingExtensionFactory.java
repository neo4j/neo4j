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

import org.neo4j.helpers.Service;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import java.io.InputStream;
import java.util.Properties;

@Service.Implementation(KernelExtensionFactory.class)
public class MonitorLoggingExtensionFactory  extends KernelExtensionFactory<MonitorLoggingExtensionFactory.Dependencies> {

    static final String KEY = "kernel monitor logging";
    private static final String filename = "/org/neo4j/ext/monitorlogging/monitorlogging.properties";

    public MonitorLoggingExtensionFactory()
    {
        super( KEY );
    }

    public interface Dependencies
    {
        Logging getLogging();

        Monitors getMonitors();
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        Logging logging = dependencies.getLogging();
        Properties props = loadProperties( logging );
        return new MonitorLoggingExtension( props, logging, dependencies.getMonitors() );
    }

    private Properties loadProperties( Logging logging )
    {
        Properties props = new Properties();
        try
        {
            InputStream resource = getClass().getResourceAsStream( filename );
            if ( resource != null )
            {
                props.load( resource );
            }
        }
        catch ( Exception e )
        {
            logging.getMessagesLog( getClass() ).warn( "Unable to read the log monitors property file: " + filename, e );
        }
        return props;
    }
}
