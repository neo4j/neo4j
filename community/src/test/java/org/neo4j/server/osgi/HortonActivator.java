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

import org.neo4j.server.osgi.services.ExampleHostService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class HortonActivator implements BundleActivator
{
    private ServiceRegistration hortonCommRegistration;
    public int whovilleCommunicationCount = 0;

    public void start( BundleContext bundleContext ) throws Exception
    {
        ExampleHostService hortonCommunicator = new ExampleHostService() {
            public String askHorton( String aQuestion )
            {
                System.out.println("100 percent!");
                whovilleCommunicationCount++;
                return "a person is a person, no matter how small";
            }
        };
        hortonCommRegistration = bundleContext.registerService(
            ExampleHostService.class.getName(), hortonCommunicator, null);

        System.out.println("Horton is listening to the Whos");
    }

    public void stop( BundleContext bundleContext ) throws Exception
    {
        hortonCommRegistration.unregister();
        System.out.println("Horton has given up on the Whos");
    }
}
