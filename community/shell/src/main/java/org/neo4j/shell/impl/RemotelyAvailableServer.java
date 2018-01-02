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
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

import org.neo4j.shell.Output;
import org.neo4j.shell.Response;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.TabCompletion;
import org.neo4j.shell.Welcome;

/**
 * The remote aspect of a {@link ShellServer}.
 * 
 * @author Mattias Persson
 */
class RemotelyAvailableServer extends UnicastRemoteObject implements ShellServer
{
    private final ShellServer actual;

    RemotelyAvailableServer( ShellServer actual ) throws RemoteException
    {
        super();
        this.actual = actual;
    }

    @Override
    public String getName() throws RemoteException
    {
        return actual.getName();
    }

    @Override
    public Response interpretLine( Serializable clientId, String line, Output out ) throws ShellException,
            RemoteException
    {
        return actual.interpretLine( clientId, line, out );
    }

    @Override
    public Serializable interpretVariable( Serializable clientId, String key )
            throws ShellException, RemoteException
    {
        return actual.interpretVariable( clientId, key );
    }

    @Override
    public void terminate( Serializable clientID ) throws RemoteException
    {
        actual.terminate( clientID );
    }

    @Override
    public Welcome welcome( Map<String, Serializable> initialSession ) throws RemoteException, ShellException
    {
        return actual.welcome( initialSession );
    }
    
    @Override
    public void leave( Serializable clientID ) throws RemoteException
    {
        actual.leave( clientID );
    }

    @Override
    public void shutdown() throws RemoteException
    {
        try
        {
            unexportObject( this, true );
        }
        catch ( NoSuchObjectException e )
        {
            // Ok
        }
    }

    @Override
    public void makeRemotelyAvailable( int port, String name ) throws RemoteException
    {
        makeRemotelyAvailable( "localhost", port, name );
    }
    
    @Override
    public void makeRemotelyAvailable( String host, int port, String name ) throws RemoteException
    {
        RmiLocation.location( host, port, name ).bind( this );
    }

    @Override
    public String[] getAllAvailableCommands() throws RemoteException
    {
        return actual.getAllAvailableCommands();
    }

    @Override
    public TabCompletion tabComplete( Serializable clientId, String partOfLine ) throws ShellException,
            RemoteException
    {
        return actual.tabComplete( clientId, partOfLine );
    }

    @Override
    public void setSessionVariable( Serializable clientID, String key, Object value ) throws RemoteException, ShellException
    {
        actual.setSessionVariable( clientID, key, value );
    }
}
