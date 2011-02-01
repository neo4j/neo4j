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
package org.neo4j.ext.udc.impl;

import org.neo4j.ext.udc.UdcProperties;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

import java.util.Properties;
import java.util.Timer;

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
public class UdcExtensionImpl extends KernelExtension implements UdcProperties
{

    public static final String UDC_SOURCE_DISTRIBUTION_KEY = "neo4j.ext.udc.host";

    /**
     * Delay, in milliseconds, before the first UDC update is sent.
     * <p/>
     * Defaults to 10 minutes.
     */
    private int firstDelay = 10 * 1000 * 60;

    /**
     * Millisecond interval for subsequent updates.
     * <p/>
     * Defaults to 24 hours.
     */
    private int interval = 1000 * 60 * 60 * 24;

    /**
     * Host address to which UDC updates will be sent.
     */
    private String hostAddress = "udc.neo4j.org";

    /**
     * Disable the extension.
     * <p/>
     * Defaults to false.
     */
    private boolean disabled = false;

    private Timer timer;
    private String source;

    /**
     * No-arg constructor, sets the extension key to "kernel udc".
     */
    public UdcExtensionImpl()
    {
        super( "kernel udc" );
    }

    /**
     * Lifecycle load event, which occurs during the startup of
     * a new GraphDbInstance.
     * <p/>
     * Configuration information is retrieved from the KernelData,
     * then a daemon thread is started to periodically collect
     * and submit usage data.
     *
     * @param kernel reference to the loading graph database kernel.
     */
    @Override
    protected void load( KernelData kernel )
    {
        configure( kernel.getConfig() );

        // ABK: a hack to register this extension with the kernel, which
        // only knows about extensions that have a saved state
        kernel.setState( this, new Object() );

        if ( !disabled )
        {
            timer = new Timer();
            NeoStoreXaDataSource ds = (NeoStoreXaDataSource)kernel.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( "nioneodb" );
            boolean crashPing = ds.getXaContainer().getLogicalLog().wasNonClean();
            String storeId = Long.toHexString( ds.getRandomIdentifier() );
            UdcTimerTask task = new UdcTimerTask( hostAddress, Version.VERSION.getRevision(), storeId, source, crashPing );
            timer.scheduleAtFixedRate( task, firstDelay, interval );
        }
    }

    @Override
    protected void unload( KernelData kernel )
    {
        if ( timer != null )
        {
            timer.cancel();
        }
    }

    /**
     * Attempt to retrieve configuration provided by user.
     * <p/>
     * Configuration precedence is in this order:
     * <p/>
     * <ol>
     * <li>value from config</li>
     * <li>system property</li>
     * <li>hard-coded default value</li>
     * <ol>
     *
     * @param config user defined configuration parameters
     */
    private void configure( Config config )
    {
        Properties props = loadSystemProperties();

        MyConfig configuration = new MyConfig( config, props );

        try
        {
            firstDelay = configuration.getInt( FIRST_DELAY_CONFIG_KEY, "600000" );
        } catch ( Exception e )
        {
            ;
        }
        try
        {
            interval = configuration.getInt( INTERVAL_CONFIG_KEY, "86400000" );
        } catch ( Exception e )
        {
            ;
        }

        try
        {
            hostAddress = configuration.getString( UDC_HOST_ADDRESS_KEY, hostAddress );
        } catch ( Exception e )
        {
            ;
        }

        try
        {
            disabled = configuration.getBool( UDC_DISABLE_KEY, "false" );
        } catch ( Exception e )
        {
            ;
        }

        try
        {
            source = configuration.getString( UDC_SOURCE_KEY, null );
        } catch ( Exception e )
        {
            ;
        }
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
            sysProps.load( getClass().getResourceAsStream( "/org/neo4j/ext/udc/udc.properties" ) );
        } catch ( Exception e )
        {
            System.err.println( "failed to load udc.properties, because: " + e );
            ; // fail silently,
        }
        return sysProps;
    }
}
