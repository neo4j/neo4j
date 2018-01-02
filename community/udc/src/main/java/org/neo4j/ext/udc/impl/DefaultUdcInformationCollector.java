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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.util.OsBeanUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.ext.udc.UdcConstants.CLUSTER_HASH;
import static org.neo4j.ext.udc.UdcConstants.DATABASE_MODE;
import static org.neo4j.ext.udc.UdcConstants.DISTRIBUTION;
import static org.neo4j.ext.udc.UdcConstants.EDITION;
import static org.neo4j.ext.udc.UdcConstants.HEAP_SIZE;
import static org.neo4j.ext.udc.UdcConstants.ID;
import static org.neo4j.ext.udc.UdcConstants.LABEL_IDS_IN_USE;
import static org.neo4j.ext.udc.UdcConstants.MAC;
import static org.neo4j.ext.udc.UdcConstants.NODE_IDS_IN_USE;
import static org.neo4j.ext.udc.UdcConstants.NUM_PROCESSORS;
import static org.neo4j.ext.udc.UdcConstants.OS_PROPERTY_PREFIX;
import static org.neo4j.ext.udc.UdcConstants.PROPERTY_IDS_IN_USE;
import static org.neo4j.ext.udc.UdcConstants.REGISTRATION;
import static org.neo4j.ext.udc.UdcConstants.RELATIONSHIP_IDS_IN_USE;
import static org.neo4j.ext.udc.UdcConstants.REVISION;
import static org.neo4j.ext.udc.UdcConstants.SERVER_ID;
import static org.neo4j.ext.udc.UdcConstants.SOURCE;
import static org.neo4j.ext.udc.UdcConstants.TAGS;
import static org.neo4j.ext.udc.UdcConstants.TOTAL_MEMORY;
import static org.neo4j.ext.udc.UdcConstants.UDC_PROPERTY_PREFIX;
import static org.neo4j.ext.udc.UdcConstants.UNKNOWN_DIST;
import static org.neo4j.ext.udc.UdcConstants.USER_AGENTS;
import static org.neo4j.ext.udc.UdcConstants.VERSION;

public class DefaultUdcInformationCollector implements UdcInformationCollector
{
    private final Config config;
    private final UsageData usageData;
    @SuppressWarnings("deprecation")
    private final IdGeneratorFactory idGeneratorFactory;

    private String storeId;
    private boolean crashPing;

    public DefaultUdcInformationCollector( Config config, DataSourceManager xadsm,
            IdGeneratorFactory idGeneratorFactory, StartupStatistics startupStats, UsageData usageData )
    {
        this.config = config;
        this.usageData = usageData;
        this.idGeneratorFactory = idGeneratorFactory;
        final StartupStatistics startupStatistics = startupStats;

        if ( xadsm != null )
        {
            xadsm.addListener( new DataSourceManager.Listener()
            {
                @Override
                public void registered( NeoStoreDataSource ds )
                {
                    crashPing = startupStatistics.numberOfRecoveredTransactions() > 0;
                    storeId = Long.toHexString( ds.getRandomIdentifier() );
                }

                @Override
                public void unregistered( NeoStoreDataSource ds )
                {
                    crashPing = false;
                    storeId = null;
                }
            } );
        }
    }

    public static String filterVersionForUDC( String version )
    {
        if ( !version.contains( "+" ) )
        {
            return version;
        }
        return version.substring( 0, version.indexOf( "+" ) );
    }

    @Override
    public Map<String, String> getUdcParams()
    {
        String classPath = getClassPath();

        Map<String, String> udcFields = new HashMap<>();

        add( udcFields, ID, storeId );
        add( udcFields, VERSION, filterVersionForUDC( usageData.get( UsageDataKeys.version ) ) );
        add( udcFields, REVISION, filterVersionForUDC( usageData.get( UsageDataKeys.revision ) ) );

        add( udcFields, EDITION, usageData.get( UsageDataKeys.edition ).name().toLowerCase() );
        add( udcFields, SOURCE, config.get( UdcSettings.udc_source ) );
        add( udcFields, REGISTRATION, config.get( UdcSettings.udc_registration_key ) );
        add( udcFields, DATABASE_MODE, usageData.get( UsageDataKeys.operationalMode ).name() );
        add( udcFields, SERVER_ID, usageData.get( UsageDataKeys.serverId ) );
        add( udcFields, USER_AGENTS, toCommaString( usageData.get( UsageDataKeys.clientNames ) ) );

        add( udcFields, TAGS, determineTags( jarNamesForTags, classPath ) );
        add( udcFields, CLUSTER_HASH, determineClusterNameHash() );

        add( udcFields, MAC, determineMacAddress() );
        add( udcFields, DISTRIBUTION, determineOsDistribution() );
        add( udcFields, NUM_PROCESSORS, determineNumberOfProcessors() );
        add( udcFields, TOTAL_MEMORY, determineTotalMemory() );
        add( udcFields, HEAP_SIZE, determineHeapSize() );

        add( udcFields, NODE_IDS_IN_USE, determineNodesIdsInUse() );
        add( udcFields, RELATIONSHIP_IDS_IN_USE, determineRelationshipIdsInUse() );
        add( udcFields, LABEL_IDS_IN_USE, determineLabelIdsInUse() );
        add( udcFields, PROPERTY_IDS_IN_USE, determinePropertyIdsInUse() );

        udcFields.putAll( determineSystemProperties() );
        return udcFields;
    }

    private String determineOsDistribution()
    {
        if ( System.getProperties().getProperty( "os.name", "" ).equals( "Linux" ) )
        {
            return searchForPackageSystems();
        }
        else
        {
            return UNKNOWN_DIST;
        }
    }

    static String searchForPackageSystems()
    {
        try
        {
            if ( new File( "/bin/rpm" ).exists() )
            {
                return "rpm";
            }
            if ( new File( "/usr/bin/dpkg" ).exists() )
            {
                return "dpkg";
            }
        }
        catch ( Exception e )
        {
            // ignore
        }
        return UNKNOWN_DIST;
    }

    private Integer determineClusterNameHash()
    {
        try
        {
            Class<?> settings = Class.forName( "org.neo4j.cluster.ClusterSettings" );
            Setting setting = (Setting) settings.getField( "cluster_name" ).get( null );
            Object name = config.get( setting );
            return name != null ? Math.abs( name.hashCode() % Integer.MAX_VALUE ) : null;
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    private final Map<String, String> jarNamesForTags = MapUtil.stringMap( "spring-", "spring",
            "(javax.ejb|ejb-jar)", "ejb", "(weblogic|glassfish|websphere|jboss)", "appserver",
            "openshift", "openshift", "cloudfoundry", "cloudfoundry",
            "(junit|testng)", "test",
            "jruby", "ruby", "clojure", "clojure", "jython", "python", "groovy", "groovy",
            "(tomcat|jetty)", "web",
            "spring-data-neo4j", "sdn" );

    private String determineTags( Map<String, String> jarNamesForTags, String classPath )
    {
        StringBuilder result = new StringBuilder();
        for ( Map.Entry<String, String> entry : jarNamesForTags.entrySet() )
        {
            final Pattern pattern = Pattern.compile( entry.getKey() );
            if ( pattern.matcher( classPath ).find() )
            {
                result.append( "," ).append( entry.getValue() );
            }
        }
        if ( result.length() == 0 )
        {
            return null;
        }
        return result.substring( 1 );
    }

    private String getClassPath()
    {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        return runtime.getClassPath();
    }

    private String determineMacAddress()
    {
        String formattedMac = "0";
        try
        {
            InetAddress address = InetAddress.getLocalHost();
            NetworkInterface ni = NetworkInterface.getByInetAddress( address );
            if ( ni != null )
            {
                byte[] mac = ni.getHardwareAddress();
                if ( mac != null )
                {
                    StringBuilder sb = new StringBuilder( mac.length * 2 );
                    Formatter formatter = new Formatter( sb );
                    for ( byte b : mac )
                    {
                        formatter.format( "%02x", b );
                    }
                    formattedMac = sb.toString();
                }
            }
        }
        catch ( Throwable t )
        {
            //
        }

        return formattedMac;
    }

    private int determineNumberOfProcessors()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    private long determineTotalMemory()
    {
        return OsBeanUtil.getTotalPhysicalMemory();
    }

    private long determineHeapSize()
    {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private long determineNodesIdsInUse()
    {
        return getNumberOfIdsInUse( IdType.NODE );
    }

    private long determineLabelIdsInUse()
    {
        return getNumberOfIdsInUse( IdType.LABEL_TOKEN );
    }

    private long determinePropertyIdsInUse()
    {
        return getNumberOfIdsInUse( IdType.PROPERTY );
    }

    private long determineRelationshipIdsInUse()
    {
        return getNumberOfIdsInUse( IdType.RELATIONSHIP );
    }

    private long getNumberOfIdsInUse( IdType type )
    {
        return idGeneratorFactory.get( type ).getNumberOfIdsInUse();
    }

    private String toCommaString( Object values )
    {
        StringBuilder result = new StringBuilder();
        if ( values instanceof Iterable )
        {
            for ( Object agent : (Iterable) values )
            {
                if ( agent == null )
                {
                    continue;
                }
                if ( result.length() > 0 )
                {
                    result.append( "," );
                }
                result.append( agent );
            }
        }
        else
        {
            result.append( values );
        }
        return result.toString();
    }

    private void add( Map<String, String> udcFields, String name, Object value )
    {
        if ( value == null )
        {
            return;
        }
        String str = value.toString().trim();
        if ( str.isEmpty() )
        {
            return;
        }
        udcFields.put( name, str );
    }

    private String removeUdcPrefix( String propertyName )
    {
        if ( propertyName.startsWith( UDC_PROPERTY_PREFIX ) )
        {
            return propertyName.substring( UDC_PROPERTY_PREFIX.length() + 1 );
        }
        return propertyName;
    }

    private String sanitizeUdcProperty( String propertyValue )
    {
        return propertyValue.replace( ' ', '_' );
    }

    private Map<String, String> determineSystemProperties()
    {
        Map<String, String> relevantSysProps = new HashMap<>();
        Properties sysProps = System.getProperties();
        Enumeration sysPropsNames = sysProps.propertyNames();
        while ( sysPropsNames.hasMoreElements() )
        {
            String sysPropName = (String) sysPropsNames.nextElement();
            if ( sysPropName.startsWith( UDC_PROPERTY_PREFIX ) || sysPropName.startsWith( OS_PROPERTY_PREFIX ) )
            {
                String propertyValue = sysProps.getProperty( sysPropName );
                relevantSysProps.put( removeUdcPrefix( sysPropName ), sanitizeUdcProperty( propertyValue ) );
            }
        }
        return relevantSysProps;
    }

    @Override
    public String getStoreId()
    {
        return storeId;
    }

    @Override
    public boolean getCrashPing()
    {
        return crashPing;
    }
}
