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

import org.neo4j.ext.udc.Edition;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.regex.Pattern;

import static org.neo4j.ext.udc.UdcConstants.*;

public class UdcInformationCollector {
    private final Config config;
    private final KernelData kernel;
    private final String storeId;
    private final boolean crashPing;

    public UdcInformationCollector(Config config, KernelData kernel) {
        this.config = config;
        this.kernel = kernel;
        NeoStoreXaDataSource ds = kernel.graphDatabase().getXaDataSourceManager().getNeoStoreDataSource();
        crashPing = ds.getXaContainer().getLogicalLog().wasNonClean();
        storeId = Long.toHexString(ds.getRandomIdentifier());
    }

    public Map<String, String> getUdcParams() {
        String classPath = getClassPath();

        Map<String, String> udcFields = new HashMap<String, String>();

        add(udcFields, ID, storeId);
        add(udcFields, VERSION, kernel.version().getReleaseVersion());
        add(udcFields, REVISION, kernel.version().getRevision());

        add(udcFields, EDITION, determineEdition(classPath));
        add(udcFields, TAGS, determineTags(jarNamesForTags, classPath));
        add(udcFields, CLUSTER_HASH, determineClusterNameHash());
        add(udcFields, SOURCE, config.<String>get(GraphDatabaseSettings.udc_source));
        add(udcFields, REGISTRATION, config.<String>get(GraphDatabaseSettings.udc_registration_key));
        add(udcFields, MAC, determineMacAddress());
        add(udcFields, DISTRIBUTION, determineOsDistribution());

        udcFields.putAll(determineSystemProperties());
        return udcFields;
    }

    private String determineOsDistribution() {
        if (System.getProperties().getProperty("os.name","").equals("Linux")) {
            return searchForPackageSystems();
        } else {
            return UNKNOWN_DIST;
        }
    }

    static String searchForPackageSystems() {
        try
        {
            if (new File("/bin/rpm").exists())
            {
                return "rpm";
            }
            if (new File("/usr/bin/dpkg").exists())
            {
                return "dpkg";
            }
        } catch (Exception e)
        {
            // ignore
        }
        return UNKNOWN_DIST;
    }

    private Integer determineClusterNameHash() {
        try
        {
            Class<?> haSettings = Class.forName("org.neo4j.kernel.ha.HaSettings");
            GraphDatabaseSetting.StringSetting setting = (GraphDatabaseSetting.StringSetting) haSettings.getField("cluster_name").get(null);
            String name = config.get(setting);
            return name!=null ? Math.abs(name.hashCode()) : null;
        } catch(Exception e) {
            return null;
        }
    }

    private org.neo4j.ext.udc.Edition determineEdition(String classPath) {
        if (classPath.contains("neo4j-ha")) return org.neo4j.ext.udc.Edition.enterprise;
        if (classPath.contains("neo4j-management")) return org.neo4j.ext.udc.Edition.advanced;
        return Edition.community;
    }

    private final Map<String,String> jarNamesForTags = MapUtil.stringMap("spring-", "spring", "(javax.ejb|ejb-jar)", "ejb", "(weblogic|glassfish|websphere|jboss)", "appserver",
            "openshift", "openshift", "cloudfoundry", "cloudfoundry",
            "(junit|testng)", "test",
            "jruby", "ruby", "clojure", "clojure", "jython", "python", "groovy", "groovy",
            "(tomcat|jetty)", "web");

    private String determineTags(Map<String, String> jarNamesForTags, String classPath) {
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

    private String getClassPath() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        return runtime.getClassPath();
    }

    private String determineMacAddress() {
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

    private void add(Map<String, String> udcFields, String name, Object value) {
        if ( value == null ) return;
        String str = value.toString().trim();
        if (str.isEmpty()) return;
        udcFields.put(name, str);
    }

    private String removeUdcPrefix(String propertyName) {
        if (propertyName.startsWith(UDC_PROPERTY_PREFIX)) {
            return propertyName.substring(UDC_PROPERTY_PREFIX.length() + 1);
        }
        return propertyName;
    }

    private String sanitizeUdcProperty(String propertyValue) {
        return propertyValue.replace(' ', '_');
    }

    private Map<String, String> determineSystemProperties()
    {
        Map<String, String> relevantSysProps = new HashMap<String, String>();
        Properties sysProps = System.getProperties();
        Enumeration sysPropsNames = sysProps.propertyNames();
        while ( sysPropsNames.hasMoreElements() )
        {
            String sysPropName = (String)sysPropsNames.nextElement();
            if (sysPropName.startsWith(UDC_PROPERTY_PREFIX) || sysPropName.startsWith(OS_PROPERTY_PREFIX))
            {
                String propertyValue = sysProps.getProperty(sysPropName);
                relevantSysProps.put(removeUdcPrefix(sysPropName), sanitizeUdcProperty(propertyValue));
            }
        }
        return relevantSysProps;
    }

    public String getStoreId() {
        return storeId;
    }

    public boolean getCrashPing() {
        return crashPing;
    }
}
