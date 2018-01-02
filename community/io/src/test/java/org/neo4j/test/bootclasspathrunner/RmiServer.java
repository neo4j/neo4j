/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test.bootclasspathrunner;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

class RmiServer implements AutoCloseable
{
    // The Registry is only initialised once per JVM, because otherwise RMI will complain
    private static Registry registry;
    private static int port = Registry.REGISTRY_PORT;

    private List<String> boundNames = new ArrayList<>();

    public RmiServer()
    {
        createRegistry();

    }

    private static synchronized void createRegistry()
    {
        while ( registry == null )
        {
            try
            {
                registry = LocateRegistry.createRegistry( port );
            }
            catch ( RemoteException e )
            {
                port++;
                if ( port > Registry.REGISTRY_PORT + 4000 )
                {
                    throw new RuntimeException( e );
                }
            }
        }
    }

    public int getPort()
    {
        return port;
    }

    public void export( String name, Remote remote )
    {
        try
        {
            Remote stub = UnicastRemoteObject.exportObject( remote, port );
            registry.rebind( name, stub );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close() throws Exception
    {
        for ( String name : registry.list() )
        {
            registry.unbind( name );
        }
    }
}
