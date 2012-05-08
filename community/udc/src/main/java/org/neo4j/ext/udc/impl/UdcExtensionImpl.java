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
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.regex.Pattern;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;
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
        return UdcSettings.class;
    }

    @Override
    protected UdcTimerTask load( KernelData kernel )
    {
        if(timer != null) {
            timer.cancel();
        }
        
        Map<String, String> conf = loadSystemProperties();
        conf.putAll( kernel.getConfigParams());
        Config config = new Config( conf );

        if ( !config.getBoolean( UdcSettings.udc_enabled )) return null;

        int firstDelay = config.getInteger( UdcSettings.first_delay);
        int interval = config.getInteger( UdcSettings.interval );
        String hostAddress = config.get( UdcSettings.udc_host );
        String source = config.get( UdcSettings.udc_source );
        String registration = config.get( UdcSettings.udc_registration_key );

        NeoStoreXaDataSource ds = kernel.graphDatabase().getXaDataSourceManager().getNeoStoreDataSource();
        boolean crashPing = ds.getXaContainer().getLogicalLog().wasNonClean();
        String storeId = Long.toHexString( ds.getRandomIdentifier() );
        String version = kernel.version().getRevision();
        if ( version.equals( "" ) ) version = kernel.version().getVersion();
        UdcTimerTask task = new UdcTimerTask( hostAddress, version, storeId, source, crashPing, registration, formattedMacAddy(), determineTags() );
        
        timer = new Timer( "Neo4j UDC Timer", /*isDaemon=*/true );
        timer.scheduleAtFixedRate( task, firstDelay, interval );
        
        return task;
    }

    private final Map<String,String> jarNamesForTags = MapUtil.stringMap("spring-", "spring", "(javax.ejb|ejb-jar)","ejb", "(weblogic|glassfish|websphere|jboss)","appserver",
            "openshift","openshift","cloudfoundry","cloudfoundry",
            "(junit|testng)", "test",
            "jruby", "ruby","clojure","clojure","jython","python","groovy","groovy",
            "(tomcat|jetty)","web");

    private String determineTags() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        final String classPath = runtime.getClassPath();
        StringBuilder result=new StringBuilder();
        for (Map.Entry<String, String> entry : jarNamesForTags.entrySet())
        {
            final Pattern pattern = Pattern.compile(entry.getKey());
            if (pattern.matcher(classPath).find())
            {
                result.append(",").append(entry.getValue());
            }
        }
        if (result.length() == 0) return null;
        return result.substring(1);
    }

    @Override
    protected void unload( UdcTimerTask task )
    {
        if(timer != null) {
            timer.cancel();
        }
    }

    private Map<String,String> loadSystemProperties()
    {
        Properties sysProps = new Properties( );
        HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
        try
        {
            InputStream resource = getClass().getResourceAsStream( "/org/neo4j/ext/udc/udc.properties" );
            if ( resource != null )
            {
                sysProps.load( resource );
                for( Map.Entry<Object, Object> objectObjectEntry : sysProps.entrySet() )
                {
                    stringStringHashMap.put( objectObjectEntry.getKey().toString(), objectObjectEntry.getValue().toString() );
                }
            }
        }
        catch ( Exception e )
        {
            System.err.println( "failed to load udc.properties, because: " + e );
        }
        return stringStringHashMap;
    }

    private String formattedMacAddy() {
        String formattedMac = "0";
        try {
            InetAddress address = InetAddress.getLocalHost();
            NetworkInterface ni = NetworkInterface.getByInetAddress(address);
            if (ni != null) {
                byte[] mac = ni.getHardwareAddress();
                if (mac != null) {
                    StringBuilder sb = new StringBuilder(mac.length * 2);
                    Formatter formatter = new Formatter(sb);
                    for (byte b : mac) {
                        formatter.format("%02x", b);
                    }
                    formattedMac = sb.toString();
                }
            }
        } catch (Throwable t) {
            //
        }

        return formattedMac;
    }
}
