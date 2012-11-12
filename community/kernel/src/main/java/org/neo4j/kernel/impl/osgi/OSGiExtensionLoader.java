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
package org.neo4j.kernel.impl.osgi;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.BundleListener;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

/**
 * An OSGi-friendly extension loader, the OSGiExtensionLoader uses normal OSGi
 * service discovery to find published services.
 *
 * For Bundles that aren't OSGi-prepared Bundles (but rather plain old jar
 * files) this extension loader will read the <code>META-INF/services</code>
 * specifications and publish those services through the OSGi service discovery.
 */
public class OSGiExtensionLoader implements BundleListener
{
    private final BundleContext bc;
    @SuppressWarnings( "unchecked" )
    private final ConcurrentMap<Long, BundleServiceProvider> providers = new ConcurrentHashMap();

    OSGiExtensionLoader( BundleContext bc )
    {
        this.bc = bc;
    }

    public <T> Iterable<T> loadExtensionsOfType( Class<T> type )
    {
        try
        {
            System.out.println( "Kernel: attempting to load extensions of type " + type.getName() );
            ServiceReference[] services = bc.getServiceReferences( type.getName(), null );
            if ( services != null )
            {
                Collection<T> serviceCollection = new LinkedList<T>();
                for ( ServiceReference sr : services )
                {
                    @SuppressWarnings( "unchecked" ) T service = (T) bc.getService( sr );
                    serviceCollection.add( service );
                }
                return serviceCollection;
            }
            else
            {
                return null;
            }
        }
        catch ( InvalidSyntaxException e )
        {
            System.out.println( "Failed to load extensions of type: " + type );
            e.printStackTrace();
        }

        return null;
    }

    public void bundleChanged( BundleEvent event )
    {
        switch ( event.getType() )
        {
        case BundleEvent.STARTING:
        case BundleEvent.STARTED:
            started( event.getBundle() );
            break;
        case BundleEvent.STOPPING:
            stopping( event.getBundle() );
            break;
        }
    }

    void started( Bundle bundle )
    {
        providers.putIfAbsent( bundle.getBundleId(), new BundleServiceProvider( bundle ) );
    }

    void stopping( Bundle bundle )
    {
        providers.remove( bundle.getBundleId() );
    }

    private class BundleServiceProvider
    {
        private final Bundle bundle;

        BundleServiceProvider( Bundle bundle )
        {
            this.bundle = bundle;
        }
    }
}
