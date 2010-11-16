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

import java.io.File;
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
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;

/**
 * Container for an embedded OSGi framework.
 */
public class OSGiContainer
{
    public static final String DEFAULT_BUNDLE_DIRECTORY = "bundles";
    public static final String DEFAULT_CACHE_DIRECTORY = "cache";

    private Framework osgiFramework;
    private HostBridge bridge;
    private Logger log = Logger.getLogger( OSGiContainer.class );
    private String bundleDirectory;

    public OSGiContainer( String bundleDirectory, String cacheDirectory )
    {
        this.bundleDirectory = bundleDirectory;

        Map<String, Object> configMap = new HashMap<String, Object>();
        bridge = new HostBridge();
        List<HostBridge> list = new ArrayList<HostBridge>();
        list.add( bridge );

        configMap.put( FelixConstants.SYSTEMBUNDLE_ACTIVATORS_PROP, list );
        configMap.put( Constants.FRAMEWORK_STORAGE, cacheDirectory );
        configMap.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA,
            "host.service.command; version=1.0.0");

        osgiFramework = new Felix( configMap );

        File bundleDirectoryAsFile = new File( bundleDirectory );
        if ( !bundleDirectoryAsFile.exists() )
        {
            bundleDirectoryAsFile.mkdirs();
        }
    }

    public OSGiContainer()
    {
        this( DEFAULT_BUNDLE_DIRECTORY, DEFAULT_CACHE_DIRECTORY );
    }

    public void start() throws BundleException
    {
        log.info( "Starting OSGi container..." );

        osgiFramework.init();
        Map<String, String> autoConfig = new HashMap<String, String>();
        log.info( "Loading bundles from: " + new File( bundleDirectory ).getAbsolutePath() );
        autoConfig.put( AutoProcessor.AUTO_DEPLOY_DIR_PROPERY, bundleDirectory );
        autoConfig.put( AutoProcessor.AUTO_DEPLOY_ACTION_PROPERY,
                AutoProcessor.AUTO_DEPLOY_INSTALL_VALUE + "," +
                        AutoProcessor.AUTO_DEPLOY_START_VALUE
        );
        AutoProcessor.process( autoConfig, osgiFramework.getBundleContext() );
        osgiFramework.start();

        for ( Bundle b : bridge.getBundles() )
        {
            logBundleState( b );
        }
        log.info( "OSGi is ready." );
    }

    public Framework getFramework()
    {
        return osgiFramework;
    }

    private void logBundleState( Bundle b )
    {
        String bundleName = b.getSymbolicName();
        if ( bundleName == null )
            bundleName = b.getLocation();
        String state = "unknown";
        switch ( b.getState() )
        {
            case Bundle.ACTIVE:
                state = "active";
                break;
            case Bundle.INSTALLED:
                state = "installed";
                break;
            case Bundle.RESOLVED:
                state = "resolved";
                break;
        }
        log.info( "\t" + state + " " + bundleName );
    }

    public Bundle[] getBundles()
    {
        return bridge.getBundles();
    }

    public void shutdown() throws BundleException, InterruptedException
    {
        osgiFramework.stop();
        osgiFramework.waitForStop( 0 );
    }
    public String getBundleDirectory()
    {
        return bundleDirectory;
    }
}
