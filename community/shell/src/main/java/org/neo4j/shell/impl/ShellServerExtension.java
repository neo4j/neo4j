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
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Service.Implementation( KernelExtension.class )
public final class ShellServerExtension extends KernelExtension<GraphDatabaseShellServer>
{
    static final String KEY = "shell";

    public ShellServerExtension()
    {
        super( KEY );
    }

    @Override
    protected GraphDatabaseShellServer load( KernelData kernel )
    {
        return loadShell( kernel, new ShellBootstrap( kernel ) );
    }

    @Override
    protected ShellBootstrap agentArgument( String agentArg )
    {
        return ShellBootstrap.deserialize( agentArg );
    }

    @Override
    protected GraphDatabaseShellServer agentLoad( KernelData kernel, Object param )
    {
        return loadShell( kernel, (ShellBootstrap) param );
    }

    private GraphDatabaseShellServer loadShell( KernelData kernel, ShellBootstrap bootstrap )
    {
        try
        {
            return bootstrap.load( kernel.graphDatabase() );
        }
        catch ( RemoteException cause )
        {
            throw new RuntimeException( "Could not load remote shell", cause );
        }
    }

    @Override
    protected void agentVisit( KernelData kernel, GraphDatabaseShellServer state, Object param )
    {
        ( (ShellBootstrap) param ).visit( state );
    }

    @Override
    protected void unload( GraphDatabaseShellServer server )
    {
        server.shutdown();
    }

    public void enableRemoteShell( KernelData kernel, Map<String, Serializable> config )
    {
        ShellBootstrap bootstrap = new ShellBootstrap( config );
        GraphDatabaseShellServer server = getState( kernel );
        try
        {
            if ( server != null )
            {
                bootstrap.enable( server );
            }
            else
            {
                loadAgent( kernel, bootstrap );
            }
        }
        catch ( RemoteException cause )
        {
            throw new RuntimeException( "Could not load remote shell", cause );
        }
    }
    
    public ShellServer getShellServer( KernelData kernel )
    {
        return getState( kernel );
    }
}
