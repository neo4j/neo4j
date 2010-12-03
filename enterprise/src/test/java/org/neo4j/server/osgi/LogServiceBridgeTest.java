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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.osgi.bundles.noisy.NoisyBundle;
import org.osgi.framework.*;
import org.osgi.service.log.LogService;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;

public class LogServiceBridgeTest
{
    OSGiContainer container;
    private InMemoryAppender logAppender;
    private File bundleDirectory;
    private File cacheDirectory;

    @Before
    public void cleanupFrameworkDirectories() throws IOException
    {
        // set up the logging
        logAppender = new InMemoryAppender( LogServiceBridge.log );

        // Don't assume that target directory exists (like when running in an IDE)
        File targetDirectory = new File( "target" + File.separator + "osgi" );
        if ( !targetDirectory.exists() )
            targetDirectory.mkdirs();

        bundleDirectory = new File( targetDirectory, OSGiContainer.DEFAULT_BUNDLE_DIRECTORY );
        FileUtils.deleteDirectory( bundleDirectory );

        cacheDirectory = new File( targetDirectory, OSGiContainer.DEFAULT_CACHE_DIRECTORY );
        FileUtils.deleteDirectory( cacheDirectory );

        this.container = new OSGiContainer( bundleDirectory.getPath(), cacheDirectory.getPath() );
    }

    @After
    public void shutdownThenDumpLog() throws BundleException, InterruptedException
    {
        this.container.shutdown();
        System.out.println( logAppender.toString() );
    }

    @Test
    public void shouldProvideAnOSGiLogServiceImplementation() throws BundleException, InvalidSyntaxException
    {
        container.start();

        ServiceReference[] registeredServices = container.getFramework().getRegisteredServices();
        assertNotNull( registeredServices );

        Bundle systemBundle = container.getBundles()[0];
        BundleContext bundleContext = systemBundle.getBundleContext();

        ServiceReference[] hostServices = bundleContext.getServiceReferences(
                LogService.class.getName(), null );
        LogService service = (LogService) bundleContext.getService( hostServices[0] );
        assertNotNull( service );

    }

    @Test
    public void shouldLogMessagesFromBundles() throws BundleException, IOException
    {
        NoisyBundle noisyBundle = new NoisyBundle();
        noisyBundle.produceJar( container.getBundleDirectory(), "noisy.jar" );
        container.start();

        assertThat( logAppender.toString(), containsString( noisyBundle.getMessageSentToLogService() ) );

    }
}
