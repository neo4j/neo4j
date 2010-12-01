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

package org.neo4j.server.osgi.bundles.consumer;

import org.neo4j.server.osgi.services.ExampleBundleService;
import org.neo4j.server.osgi.services.ExampleHostService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

/**
 * For a bundle to consume an OSGi "service" means that it
 * participates in the OSGi lifecycle and consumes an
 * implementation instance of a service interface.
 */
public class WhovilleActivator implements BundleActivator
{
    private ExampleHostService hortonCommunicator;

    public void start( BundleContext bundleContext ) throws Exception
    {
        hortonCommunicator = (ExampleHostService)bundleContext.getService( bundleContext.getServiceReference( ExampleHostService.class.toString()) );

        System.out.println( "OSGi service consumer bundle started" );
    }

    public void stop( BundleContext bundleContext ) throws Exception
    {
        System.out.println( "OSGi service consumer bundle stopped" );
    }
}
