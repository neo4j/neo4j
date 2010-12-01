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
import java.util.*;

import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.FelixConstants;
import org.apache.felix.main.AutoProcessor;
import org.neo4j.ext.udc.impl.osgi.OSGiActivator;
import org.neo4j.server.logging.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;

/**
 * Container for an embedded OSGi framework.
 */
public class OSGiContainer
{
    public static Logger log = Logger.getLogger( OSGiContainer.class );

    public static final String DEFAULT_BUNDLE_DIRECTORY = "bundles";
    public static final String DEFAULT_CACHE_DIRECTORY = "cache";

    private Framework osgiFramework;
    private String bundleDirectory;

    public OSGiContainer( String bundleDirectory, String cacheDirectory )
    {
        this( bundleDirectory, cacheDirectory, new HostBridge() );
    }

    public OSGiContainer( String bundleDirectory, String cacheDirectory, BundleActivator... activators )
    {
        this.bundleDirectory = bundleDirectory;

        Map<String, Object> configMap = new HashMap<String, Object>();

        if ( activators != null )
        {
            List<BundleActivator> list = new ArrayList<BundleActivator>();
            list.addAll( Arrays.asList( activators ) );
            configMap.put( FelixConstants.SYSTEMBUNDLE_ACTIVATORS_PROP, list );
        }
        configMap.put( Constants.FRAMEWORK_STORAGE, cacheDirectory );
        configMap.put( Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA,
                "org.neo4j.server.osgi.services; version=1.0.0" );


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


        for ( Bundle b : osgiFramework.getBundleContext().getBundles() )
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
        return osgiFramework.getBundleContext().getBundles();
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
