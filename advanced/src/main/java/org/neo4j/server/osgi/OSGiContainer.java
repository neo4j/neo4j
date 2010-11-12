/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.osgi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.FelixConstants;
import org.apache.felix.main.AutoProcessor;
import org.neo4j.server.logging.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.launch.Framework;

/**
 */
public class OSGiContainer
{
    private Framework osgiFramework;
    private String bundledir;
    private HostBridge bridge;
    private Logger log = Logger.getLogger( OSGiContainer.class );

    public OSGiContainer( String bundleDirectory ) throws Exception
    {
        bundledir = bundleDirectory;

        Map<String, List<HostBridge>> configMap = new HashMap<String, List<HostBridge>>();
        bridge = new HostBridge();
        List<HostBridge> list = new ArrayList<HostBridge>();
        list.add( bridge );

        configMap.put( FelixConstants.SYSTEMBUNDLE_ACTIVATORS_PROP, list );

        osgiFramework = new Felix( configMap );

        // set expected system properties
    }

    public void startContainer() throws BundleException
    {
        log.info("Starting OSGi container...");
        osgiFramework.init();
        Map<String, String> autoConfig = new HashMap<String, String>();
        log.info( "Loading bundles from: " + bundledir );
        autoConfig.put( AutoProcessor.AUTO_DEPLOY_DIR_PROPERY, bundledir );
        autoConfig.put( AutoProcessor.AUTO_DEPLOY_ACTION_PROPERY,
                AutoProcessor.AUTO_DEPLOY_INSTALL_VALUE + "," +
                        AutoProcessor.AUTO_DEPLOY_START_VALUE
        );
        AutoProcessor.process( autoConfig, osgiFramework.getBundleContext() );
        osgiFramework.start();
        log.info("OSGi is ready.");
    }

    public Bundle[] getInstalledBundles()
    {
        return bridge.getBundles();
    }

    public void shutdownContainer() throws BundleException, InterruptedException
    {
        osgiFramework.stop();
        osgiFramework.waitForStop( 0 );
    }
}
