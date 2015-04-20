/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ext.udc.impl.osgi;

import java.util.Dictionary;
import java.util.Properties;

import org.neo4j.ext.udc.impl.UdcKernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;


/**
 * OSGi bundle activator to start an OSGi servicewatcher
 * for kernel extensions.
 */
public final class OSGiActivator
        implements BundleActivator
{

    /**
     * Called whenever the OSGi framework starts our bundle
     */
    public void start( BundleContext bc )
            throws Exception
    {
        Dictionary props = new Properties();
        // Register our example service implementation in the OSGi service registry
        bc.registerService( KernelExtensionFactory.class.getName(), new UdcKernelExtensionFactory(), props );
    }

    /**
     * Called whenever the OSGi framework stops our bundle
     */
    public void stop( BundleContext bc )
            throws Exception
    {
        // no need to unregister our service - the OSGi framework handles it for us
    }

}

