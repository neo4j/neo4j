/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.neo;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Set;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.EmbeddedReadOnlyNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.shell.App;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.SimpleAppServer;

public class LocalNeoShellServer extends SimpleAppServer
{
    private final String neoDirectory;
    private final boolean readOnly;
    private NeoService neoService;
    private NeoShellServer neoShellServer;

    public LocalNeoShellServer( String neoDirectory, boolean readOnly )
        throws RemoteException
    {
        super();
        this.neoDirectory = neoDirectory;
        this.readOnly = readOnly;
        
        this.neoService = this.readOnly ?
            new EmbeddedReadOnlyNeo( neoDirectory ) :
            new EmbeddedNeo( neoDirectory );
        try
        {
            this.neoShellServer = new NeoShellServer( neoService );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void shutdownServer()
    {
        if ( this.neoShellServer == null )
        {
            return;
        }

        this.neoShellServer.shutdown();
        if ( this.neoDirectory != null )
        {
            this.neoService.shutdown();
        }
        this.neoService = null;
        this.neoShellServer = null;
    }

    @Override
    public String getName()
    {
        return this.neoShellServer.getName();
    }

    @Override
    public Serializable getProperty( String key )
    {
        return this.neoShellServer.getProperty( key );
    }

    @Override
    public void setProperty( String key, Serializable value )
    {
        this.neoShellServer.setProperty( key, value );
    }
    
    @Override
    public Set<Class<? extends App>> getApps()
    {
        return this.neoShellServer.getApps();
    }

    @Override
    public App findApp( String command )
    {
        return this.neoShellServer.findApp( command );
    }

    @Override
    public String welcome()
    {
        return this.neoShellServer.welcome();
    }

    @Override
    public Serializable interpretVariable( String key, Serializable value,
        Session session ) throws ShellException, RemoteException
    {
        return this.neoShellServer.interpretVariable( key, value, session );
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        if ( this.neoShellServer != null )
        {
            this.shutdownServer();
        }
    }
}
