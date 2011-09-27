/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.util.Properties;
import java.util.Timer;

import org.neo4j.ext.udc.UdcProperties;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

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
public class UdcExtensionImpl extends KernelExtension<UdcTimerTask> implements UdcProperties
{
    static final String KEY = "kernel udc";
    public static final String UDC_SOURCE_DISTRIBUTION_KEY = "neo4j.ext.udc.host";
    /**
     * Millisecond interval for subsequent updates.
     * <p/>
     * Defaults to 24 hours.
     */
    private static final int DEFAULT_INTERVAL = 1000 * 60 * 60 * 24;

    /**
     * Delay, in milliseconds, before the first UDC update is sent.
     * <p/>
     * Defaults to 10 minutes.
     */
    private static final int DEFAULT_DELAY = 10 * 1000 * 60;

    /**
     * Host address to which UDC updates will be sent.
     */
    private static final String DEFAULT_HOST = "udc.neo4j.org";

    /**
     * No-arg constructor, sets the extension key to "kernel udc".
     */
    public UdcExtensionImpl()
    {
        super( KEY );
    }

    private static Timer timer = new Timer( "Neo4j UDC Timer", /*isDeamon=*/true );

    @Override
    protected UdcTimerTask load( KernelData kernel )
    {
        MyConfig configuration = new MyConfig( kernel.getConfig(), loadSystemProperties() );

        try
        {
            // break if disabled
            if ( configuration.getBool( UDC_DISABLE_KEY, "false" ) ) return null;
        }
        catch ( Exception e )
        {
            // default: not disabled
        }
        int firstDelay = DEFAULT_DELAY;
        int interval = DEFAULT_INTERVAL;
        String hostAddress = DEFAULT_HOST;
        String source = null;
        try
        {
            firstDelay = configuration.getInt( FIRST_DELAY_CONFIG_KEY, Integer.toString( firstDelay ) );
        }
        catch ( Exception e )
        {
            // fall back to default
        }
        try
        {
            interval = configuration.getInt( INTERVAL_CONFIG_KEY, Integer.toString( interval ) );
        }
        catch ( Exception e )
        {
            // fall back to default
        }
        try
        {
            hostAddress = configuration.getString( UDC_HOST_ADDRESS_KEY, hostAddress );
        }
        catch ( Exception e )
        {
            // fall back to default
        }
        try
        {
            source = configuration.getString( UDC_SOURCE_KEY, source );
        }
        catch ( Exception e )
        {
            // fall back to default
        }
        NeoStoreXaDataSource ds = (NeoStoreXaDataSource) kernel.getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        boolean crashPing = ds.getXaContainer().getLogicalLog().wasNonClean();
        String storeId = Long.toHexString( ds.getRandomIdentifier() );
        String version = kernel.version().getRevision();
        if ( version.equals( "" ) ) version = kernel.version().getVersion();
        UdcTimerTask task = new UdcTimerTask( hostAddress, version, storeId, source, crashPing );
        timer.scheduleAtFixedRate( task, firstDelay, interval );
        return task;
    }

    @Override
    protected void unload( UdcTimerTask task )
    {
        task.cancel();
    }

    private class MyConfig
    {
        private final Config config;
        private final Properties props;

        private MyConfig( Config config, Properties props )
        {
            this.config = config;
            this.props = props;
        }

        private String getString( String key, String defaultValue )
        {
            String result = (String)config.getParams().get( key );
            if ( result == null )
            {
                result = props.getProperty( key, defaultValue );
            }
            return result;
        }

        private int getInt( String key, String defaultValue )
        {
            String result = getString( key, defaultValue );
            return Integer.parseInt( result );
        }

        private boolean getBool( String key, String defaultValue )
        {
            String result = getString( key, defaultValue );
            return Boolean.parseBoolean( result );
        }
    }

    private Properties loadSystemProperties()
    {
        Properties sysProps = System.getProperties();
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
            System.err.println( "failed to load udc.properties, because: " + e );
        }
        return sysProps;
    }
}
