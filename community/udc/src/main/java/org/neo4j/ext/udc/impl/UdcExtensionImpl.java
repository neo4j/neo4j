/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;

/**
 * Kernel extension for UDC, the Usage Data Collector. The UDC runs as a background
 * daemon, waking up once a day to collect basic usage information about a long
 * running graph database.
 * <p/>
 * The first update is delayed to avoid needless activity during integration
 * testing and short-run applications. Subsequent updates are made at regular
 * intervals. Both times are specified in milliseconds.
 */
@Service.Implementation( KernelExtension.class )
public class UdcExtensionImpl extends KernelExtension<UdcTimerTask>
{
    static final String KEY = "kernel udc";

    /**
     * No-arg constructor, sets the extension key to "kernel udc".
     */
    public UdcExtensionImpl()
    {
        super( KEY );
    }

    private Timer timer;

    @Override
    public Class getSettingsClass()
    {
        return GraphDatabaseSettings.class;
    }

    @Override
    protected UdcTimerTask load( KernelData kernel )
    {
        if(timer != null) {
            timer.cancel();
        }

        Config config = loadConfig(kernel);

        if ( !config.getBoolean( GraphDatabaseSettings.udc_enabled )) return null;

        int firstDelay = config.getInteger( GraphDatabaseSettings.first_delay);
        int interval = config.getInteger( GraphDatabaseSettings.interval );
        String hostAddress = config.get( GraphDatabaseSettings.udc_host );

        UdcInformationCollector collector = new UdcInformationCollector(config, kernel);
        UdcTimerTask task = new UdcTimerTask( hostAddress, collector.getStoreId(), collector.getCrashPing(), collector.getUdcParams());
        
        timer = new Timer( "Neo4j UDC Timer", /*isDaemon=*/true );
        timer.scheduleAtFixedRate( task, firstDelay, interval );
        
        return task;
    }

    private Config loadConfig(KernelData kernel) {
        Properties udcProps = loadUdcProperties();
        HashMap<String, String> config = new HashMap<String, String>(kernel.getConfigParams());
        for (Map.Entry<Object, Object> entry : udcProps.entrySet()) {
            config.put((String)entry.getKey(), (String) entry.getValue());
        }
        return new Config( config );
    }


    @Override
    protected void unload( UdcTimerTask task )
    {
        if(timer != null) {
            timer.cancel();
        }
    }

    private Properties loadUdcProperties()
    {
        Properties sysProps = new Properties( );
        try
        {
            InputStream resource = getClass().getResourceAsStream( "/org/neo4j/ext/udc/udc.properties" );
            if (resource != null) {
                sysProps.load(resource);
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
