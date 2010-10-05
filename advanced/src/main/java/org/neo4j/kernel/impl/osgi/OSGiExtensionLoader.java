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

package org.neo4j.kernel.impl.osgi;

import org.neo4j.helpers.ExtensionLoader;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

import java.util.Vector;

/**
 * An OSGi-friendly ExtensionLoader, the OSGiExtensionLoader
 * uses normal OSGi service discovery to find published services.
 */
public class OSGiExtensionLoader implements ExtensionLoader {

    private BundleContext bc;

    public OSGiExtensionLoader(BundleContext bc) {
        this.bc = bc;
    }

    public <T> Iterable<T> loadExtensionsOfType(Class<T> type) {
        try {
            System.out.println("Kernel: attempting to load extensions of type " + type.getName());
            ServiceReference[] services = bc.getServiceReferences(type.getName(), null);
            if (services != null) {
                Vector<T> serviceCollection = new Vector<T>();
                for (ServiceReference sr : services) {
                   serviceCollection.add((T)bc.getService(sr));
                }
                return serviceCollection;
            } else {
                return null;
            }
        } catch (InvalidSyntaxException e) {
            System.out.println("Failed to load extensions of type: " + type);
            e.printStackTrace();
        }

        return null;
    }

}
