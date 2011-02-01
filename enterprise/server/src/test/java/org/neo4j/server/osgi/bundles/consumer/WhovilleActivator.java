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
package org.neo4j.server.osgi.bundles.consumer;

import org.neo4j.server.osgi.bundles.BundleJarProducer;
import org.neo4j.server.osgi.services.ExampleHostService;
import org.osgi.framework.*;

/**
 * For a bundle to consume an OSGi "service" means that it
 * participates in the OSGi lifecycle and consumes an
 * implementation instance of a service interface.
 * <p/>
 * This activator listens for service events, looking
 * for the registration of an ExampleHostService.
 */
public class WhovilleActivator extends BundleJarProducer implements BundleActivator, ServiceListener
{
    private ExampleHostService hortonCommunicator = null;
    private BundleContext bundleContext;

    public void start( BundleContext bundleContext ) throws Exception
    {
        this.bundleContext = bundleContext;

        synchronized (this)
        {
            bundleContext.addServiceListener( this );

            ServiceReference[] refs = bundleContext.getServiceReferences(
                    ExampleHostService.class.getName(), null );

            if ( refs != null )
            {
                hortonCommunicator = (ExampleHostService) bundleContext.getService( refs[0] );
                hortonCommunicator.askHorton( "Can you hear us?" );
            }
        }


        System.out.println( "Whoville is looking for Horton" );
    }

    public void stop( BundleContext bundleContext ) throws Exception
    {
        System.out.println( "Whoville has given up on Horton" );
    }

    @Override
    public synchronized void serviceChanged( ServiceEvent serviceEvent )
    {
        if ( serviceEvent.getType() == ServiceEvent.REGISTERED )
        {
            try
            {
                hortonCommunicator = (ExampleHostService) bundleContext.getService( serviceEvent.getServiceReference() );
                hortonCommunicator.askHorton( "Can you hear us now?" );
            } catch ( Exception e )
            {
                ; // oh, well
            }
        }
    }

    @Override
    public String getBundleSymbolicName()
    {
        return "WhovilleBundle";
    }

    @Override
    protected Class[] getExtraBundleClasses()
    {
        return new Class[0];
    }

    @Override
    public String getImportedPackages()
    {
        return super.getImportedPackages() + ", org.neo4j.server.osgi.services";
    }
}
