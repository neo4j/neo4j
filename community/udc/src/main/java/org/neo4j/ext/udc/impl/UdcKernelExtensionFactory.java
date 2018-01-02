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
package org.neo4j.ext.udc.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.udc.UsageData;

/**
 * Kernel extension for UDC, the Usage Data Collector. The UDC runs as a background
 * daemon, waking up once a day to collect basic usage information about a long
 * running graph database.
 * <p>
 * The first update is delayed to avoid needless activity during integration
 * testing and short-run applications. Subsequent updates are made at regular
 * intervals. Both times are specified in milliseconds.
 */
@Service.Implementation(KernelExtensionFactory.class)
public class UdcKernelExtensionFactory extends KernelExtensionFactory<UdcKernelExtensionFactory.Dependencies>
{
    static final String KEY = "kernel udc";

    public interface Dependencies
    {
        Config config();
        DataSourceManager dataSourceManager();
        UsageData usageData();
        IdGeneratorFactory idGeneratorFactory();
        StartupStatistics startupStats();
    }

    /**
     * No-arg constructor, sets the extension key to "kernel udc".
     */
    public UdcKernelExtensionFactory()
    {
        super( KEY );
    }

    @Override
    public Class getSettingsClass()
    {
        return UdcSettings.class;
    }

    @Override
    public Lifecycle newKernelExtension( UdcKernelExtensionFactory.Dependencies dependencies ) throws Throwable
    {
        return new UdcKernelExtension(
                loadConfig( dependencies.config() ),
                dependencies.dataSourceManager(),
                dependencies.idGeneratorFactory(),
                dependencies.startupStats(),
                dependencies.usageData(),
                new Timer( "Neo4j UDC Timer", isAlwaysDaemon() ) );
    }

    private boolean isAlwaysDaemon()
    {
        return true;
    }

    private Config loadConfig( Config config )
    {
        Properties udcProps = loadUdcProperties();
        HashMap<String, String> configParams = new HashMap<String, String>( config.getParams() );
        for ( Map.Entry<Object, Object> entry : udcProps.entrySet() )
        {
            configParams.put( (String) entry.getKey(), (String) entry.getValue() );
        }
        return new Config( configParams );
    }

    private Properties loadUdcProperties()
    {
        Properties sysProps = new Properties();
        try
        {
            InputStream resource = getClass().getResourceAsStream( "/org/neo4j/ext/udc/udc.properties" );
            if ( resource != null )
            {
                sysProps.load( resource );
            }
        }
        catch ( Exception e )
        {
            // AN: commenting out as this provides no value to the user
            //System.err.println( "failed to load udc.properties, because: " + e );
        }
        return sysProps;
    }
}
