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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.newBundle;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.withBnd;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.osgi.bundles.aware.LifecycleActivator;
import org.neo4j.server.osgi.bundles.BundleJarProducer;
import org.neo4j.server.osgi.bundles.consumer.WhovilleActivator;
import org.neo4j.server.osgi.bundles.hello.Hello;
import org.neo4j.server.osgi.bundles.service.ExampleServiceImpl;
import org.neo4j.server.osgi.bundles.service.ServiceProviderActivator;
import org.neo4j.server.osgi.services.ExampleBundleService;
import org.neo4j.server.osgi.services.ExampleHostService;
import org.ops4j.io.StreamUtils;
import org.osgi.framework.*;

public class OSGiContainerTest
{

    OSGiContainer container;
    private InMemoryAppender logAppender;
    private File bundleDirectory;
    private File cacheDirectory;

    @Before
    public void cleanupFrameworkDirectories() throws IOException
    {
        // set up the logging
        logAppender = new InMemoryAppender( OSGiContainer.log );

        // Don't assume that target directory exists (like when running in an IDE)
        File targetDirectory = new File( "target" + File.separator + "osgi" );
        if ( !targetDirectory.exists() )
            targetDirectory.mkdirs();

        bundleDirectory = new File( targetDirectory, OSGiContainer.DEFAULT_BUNDLE_DIRECTORY );
        FileUtils.deleteDirectory( bundleDirectory );

        cacheDirectory = new File( targetDirectory, OSGiContainer.DEFAULT_CACHE_DIRECTORY );
        FileUtils.deleteDirectory( cacheDirectory );

    }

    public void createContainer( BundleActivator... extraActivators )
    {
        BundleActivator[] activators = Arrays.copyOf( extraActivators, extraActivators.length + 1 );
        activators[activators.length - 1] = new HostBridge();
        this.container = new OSGiContainer( bundleDirectory.getPath(), cacheDirectory.getPath(), activators );
    }

    @After
    public void shutdownThenDumpLog() throws BundleException, InterruptedException
    {
        this.container.shutdown();
        System.out.println( logAppender.toString() );
    }

    @Test
    public void shouldCreateFrameworkDuringConstruction() throws Exception
    {
        createContainer();
        assertThat( container.getFramework(), is( notNullValue() ) );
        assertThat( container.getFramework().getState(), is( Bundle.INSTALLED ) );

    }

    @Test
    public void shouldStartMinimalFramework() throws Exception
    {
        createContainer();
        container.start();

        assertThat( container.getFramework().getState(), is( Bundle.ACTIVE ) );
    }

    @Test
    public void shouldCreateSystemBundle() throws Exception
    {
        createContainer();
        container.start();

        // The system bundle should always be bundle zero,
        // and can otherwise be identified by the system
        // packages it provides
        Bundle systemBundle = container.getBundles()[0];

        assertThat( (String) systemBundle.getHeaders().get( Constants.EXPORT_PACKAGE ), containsString( "org.osgi.framework" ) );
    }

    @Test
    public void shouldCreateBundleDirectoryDuringConstructionIfItDoesntExist() throws BundleException, InterruptedException
    {
        createContainer();
        File bundleDirectory = new File( container.getBundleDirectory() );

        assertTrue( bundleDirectory.exists() );
    }

    @Test
    public void shouldLoadLibraryBundle() throws Exception
    {
        createContainer();
        String expectedBundleSymbolicName = "HelloTinyBundle";
        InputStream bundleStream = newBundle()
                .add( Hello.class )
                .set( Constants.BUNDLE_SYMBOLICNAME, expectedBundleSymbolicName )
                .set( Constants.EXPORT_PACKAGE, "org.neo4j.server.osgi.bundles.hello" )
                .set( Constants.IMPORT_PACKAGE, "org.neo4j.server.osgi.bundles.hello" )
                .build( withBnd() );
        File helloJar = new File( container.getBundleDirectory(), "hello.jar" );
        OutputStream jarOutputStream = new FileOutputStream( helloJar );
        StreamUtils.copyStream( bundleStream, jarOutputStream, true );

        container.start();

        // should be bundle 1
        Bundle helloBundle = container.getBundles()[1];

        assertThat( (String) helloBundle.getHeaders().get( Constants.BUNDLE_SYMBOLICNAME ), is( expectedBundleSymbolicName ) );
        assertThat( container.getFramework().getState(), is( Bundle.ACTIVE ) );
    }

    @Test
    public void shouldActivateOSGiAwareBundles() throws Exception
    {
        createContainer();

        LifecycleActivator lifecycler = new LifecycleActivator();
        lifecycler.produceJar( container.getBundleDirectory(), "lifecycler.jar" );
        String expectedBundleSymbolicName = lifecycler.getBundleSymbolicName();

        container.start();

        Bundle awareBundle = container.getBundles()[1];

        assertNotNull( awareBundle );

        assertThat( awareBundle.getState(), is( Bundle.ACTIVE ) );
    }

    @Test
    public void shouldAllowAccessToOSGiServices() throws Exception
    {
        createContainer();

        ServiceProviderActivator serviceProvider = new ServiceProviderActivator();
        serviceProvider.produceJar( container.getBundleDirectory(), "service-impl.jar");
        String expectedBundleSymbolicName = serviceProvider.getBundleSymbolicName();

        container.start();

        Bundle serviceProviderBundle = container.getBundles()[1];
        assertNotNull( serviceProviderBundle );
        assertThat( serviceProviderBundle.getState(), is( Bundle.ACTIVE ) );
        assertThat( (String) serviceProviderBundle.getHeaders().get( Constants.BUNDLE_SYMBOLICNAME ), is( expectedBundleSymbolicName ) );
        // should have 1 registered service
        BundleContext bundleContext = serviceProviderBundle.getBundleContext();
        ServiceReference[] registeredServices = bundleContext.getServiceReferences(
                ExampleBundleService.class.getName(), null );
        ExampleBundleService service = (ExampleBundleService) bundleContext.getService( registeredServices[0] );
        assertThat( service, is( notNullValue() ) );
    }

    @Test
    public void shouldProvideHostServiceToOSGiBundles() throws IOException, BundleException, InvalidSyntaxException, InterruptedException
    {
        HortonActivator hortonActivator = new HortonActivator();
        createContainer( hortonActivator );

        WhovilleActivator whoville = new WhovilleActivator();
        whoville.produceJar( container.getBundleDirectory(), "whoville.jar");
        String expectedBundleSymbolicName = whoville.getBundleSymbolicName();

        container.start();

        ServiceReference[] registeredServices = container.getFramework().getRegisteredServices();
        assertNotNull( registeredServices );

        Bundle serviceConsumerBundle = container.getBundles()[1];
        BundleContext bundleContext = serviceConsumerBundle.getBundleContext();
        assertThat( (String) serviceConsumerBundle.getHeaders().get( Constants.BUNDLE_SYMBOLICNAME ), is( expectedBundleSymbolicName ) );
        assertThat( serviceConsumerBundle.getState(), is( Bundle.ACTIVE ) );

        ServiceReference[] hostServices = bundleContext.getServiceReferences(
                ExampleHostService.class.getName(), null );
        ExampleHostService service = (ExampleHostService) bundleContext.getService( hostServices[0] );
        assertNotNull( service );

        assertThat( hortonActivator.whovilleCommunicationCount, is( 1 ) );
    }

}
