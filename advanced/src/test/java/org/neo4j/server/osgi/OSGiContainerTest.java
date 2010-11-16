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

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.osgi.bundles.service.ExampleServiceImpl;
import org.neo4j.server.osgi.bundles.aware.LifecycleActivator;
import org.neo4j.server.osgi.bundles.hello.Hello;
import org.ops4j.io.StreamUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;

import java.io.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.newBundle;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.withBnd;

public class OSGiContainerTest
{

    @Before
    public void cleanupFrameworkDirectories() throws IOException
    {
        File bundleDirectory = new File( OSGiContainer.DEFAULT_BUNDLE_DIRECTORY );
        FileUtils.deleteDirectory( bundleDirectory );

        File cacheDirectory = new File( OSGiContainer.DEFAULT_CACHE_DIRECTORY );
        FileUtils.deleteDirectory( cacheDirectory );
    }

    @Test
    public void shouldCreateFrameworkDuringConstruction() throws Exception
    {
        OSGiContainer container = new OSGiContainer();

        assertThat( container.getFramework(), is( notNullValue() ) );
        assertThat( container.getFramework().getState(), is( Bundle.INSTALLED ) );

    }

    @Test
    public void shouldStartMinimalFramework() throws Exception
    {
        OSGiContainer container = new OSGiContainer();
        container.start();

        assertThat( container.getFramework().getState(), is( Bundle.ACTIVE ) );

        container.shutdown();
    }

    @Test
    public void shouldCreateSystemBundle() throws Exception
    {
        OSGiContainer container = new OSGiContainer();
        container.start();

        // The system bundle should always be bundle zero,
        // and can otherwise be identified by the system
        // packages it provides
        Bundle systemBundle = container.getBundles()[0];

        assertThat( (String) systemBundle.getHeaders().get( Constants.EXPORT_PACKAGE ), containsString( "org.osgi.framework" ) );

        container.shutdown();
    }

    @Test
    public void shouldCreateBundleDirectoryDuringConstructionIfItDoesntExist() throws BundleException, InterruptedException
    {
        OSGiContainer container = new OSGiContainer();

        File bundleDirectory = new File( container.getBundleDirectory() );

        assertTrue( bundleDirectory.exists() );

        container.shutdown();
    }

    @Test
    public void shouldLoadLibraryBundle() throws Exception
    {
        OSGiContainer container = new OSGiContainer();
        String expectedBundleSymbolicName = "HelloTinyBundle";
        InputStream bundleStream = newBundle()
                .add( Hello.class )
                .set( Constants.BUNDLE_SYMBOLICNAME, expectedBundleSymbolicName )
                .set( Constants.EXPORT_PACKAGE, "org.neo4j.server.osgi.bundles.hello" )
                .set( Constants.IMPORT_PACKAGE, "org.neo4j.server.osgi.bundles.hello" )
                .build( withBnd() );
        File helloJar = new File( container.getBundleDirectory() + "/hello.jar" );
        OutputStream jarOutputStream = new FileOutputStream( helloJar );
        StreamUtils.copyStream( bundleStream, jarOutputStream, true );

        container.start();

        // should be bundle 1
        Bundle helloBundle = container.getBundles()[1];

        assertThat( (String) helloBundle.getHeaders().get( Constants.BUNDLE_SYMBOLICNAME ), is( expectedBundleSymbolicName ) );

        container.shutdown();
    }

    @Test
    public void shouldActivateOSGiAwareBundles() throws Exception
    {
        OSGiContainer container = new OSGiContainer();
        String expectedBundleSymbolicName = "OSGiAwareBundle";
        InputStream bundleStream = newBundle()
                .add( LifecycleActivator.class )
                .set( Constants.BUNDLE_SYMBOLICNAME, expectedBundleSymbolicName )
                .set( Constants.EXPORT_PACKAGE, "org.neo4j.server.osgi.bundles.aware" )
                .set( Constants.IMPORT_PACKAGE, "org.neo4j.server.osgi.bundles.aware, org.osgi.framework" )
                .build( withBnd() );
        File awareJar = new File( container.getBundleDirectory(), "aware.jar" );
        OutputStream jarOutputStream = new FileOutputStream( awareJar );
        StreamUtils.copyStream( bundleStream, jarOutputStream, true );

        container.start();

        Bundle awareBundle = container.getBundles()[1];

        container.shutdown();
    }

    @Test
    @Ignore("until HostBridge or OSGiContainer, or someone to publish server-side packages")
    public void shouldAllowAccessToOSGiServices() throws Exception
    {
        OSGiContainer container = new OSGiContainer();
        String expectedBundleSymbolicName = "OSGiAwareBundle";
        InputStream bundleStream = newBundle()
                .add( LifecycleActivator.class )
                .add( ExampleServiceImpl.class )
                .set( Constants.BUNDLE_SYMBOLICNAME, expectedBundleSymbolicName )
                .set( Constants.EXPORT_PACKAGE, "org.neo4j.server.osgi.bundles.service" )
                .set( Constants.IMPORT_PACKAGE, "org.neo4j.server.osgi.bundles.service, org.neo4j.server.osgi.services, org.osgi.framework" )
                .build( withBnd() );
        File awareJar = new File( container.getBundleDirectory(), "aware.jar" );
        OutputStream jarOutputStream = new FileOutputStream( awareJar );
        StreamUtils.copyStream( bundleStream, jarOutputStream, true );

        container.start();

        Bundle awareBundle = container.getBundles()[1];
        // should have 1 registered service
        ExampleServiceImpl service = (ExampleServiceImpl) awareBundle.getBundleContext().getService( awareBundle.getRegisteredServices()[0] );
        assertThat( service, is( notNullValue() ) );

        container.shutdown();
    }
}
