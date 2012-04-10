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

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * This class is responsible for bootstrapping the Neo4j kernel as an OSGi
 * component. The main responsibility is to bootstrap the
 * {@link OSGiExtensionLoader}.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 * @author Andreas Kollegger <andreas.kollegger@neotechnology.com>
 */
public final class OSGiActivator implements BundleActivator
{
    public static volatile OSGiExtensionLoader osgiExtensionLoader;

    /**
     * Called whenever the OSGi framework starts our bundle
     */
    public void start( BundleContext bc ) throws Exception
    {
        // start the extension listener
        OSGiExtensionLoader loader = new OSGiExtensionLoader( bc );
        /* // TODO: enable this when OSGiExtensionLoader is implemented
         * // to support adding OSGi services from META-INF/services
        bc.addBundleListener( loader );
        for ( Bundle bundle : bc.getBundles() )
        {
            if ( bundle.getState() == Bundle.ACTIVE )
            {
                loader.started( bundle );
            }
        }
        osgiExtensionLoader = loader;
        */
    }

    /**
     * Called whenever the OSGi framework stops our bundle
     */
    public void stop( BundleContext bc ) throws Exception
    {
        // TODO: implement proper OSGi activation for Kernel
        // bc.removeBundleListener( osgiExtensionLoader );
        osgiExtensionLoader = null;
    }
}
