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
package org.neo4j.server.osgi.bundles.service;

import org.neo4j.server.osgi.bundles.BundleJarProducer;
import org.neo4j.server.osgi.services.ExampleBundleService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * For a bundle to provide an OSGi "service" means that it
 * participates in the OSGi lifecycle and publishes an
 * implementation instance of a service interface.
 */
public class ServiceProviderActivator extends BundleJarProducer implements BundleActivator
{
    private ServiceRegistration exampleServiceReference;

    public void start( BundleContext bundleContext ) throws Exception
    {
        ExampleBundleService serviceImplementaton = new ExampleServiceImpl();

        exampleServiceReference = bundleContext.registerService(
                ExampleBundleService.class.getName(), serviceImplementaton, null );

        System.out.println( "OSGi service provider bundle started" );
    }

    public void stop( BundleContext bundleContext ) throws Exception
    {
        exampleServiceReference.unregister();
        System.out.println( "OSGi service provider bundle stopped" );
    }

    @Override
    public String getBundleSymbolicName()
    {
        return "OSGiServiceProviderBundle";
    }

    @Override
    protected Class[] getExtraBundleClasses()
    {
        return new Class[] { ExampleServiceImpl.class };
    }

    @Override
    public String getImportedPackages()
    {
        return "org.neo4j.server.osgi.bundles.service, org.neo4j.server.osgi.services, org.osgi.framework";
    }
}
