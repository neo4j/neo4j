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
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.TabCompletion;

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

    public String getName() throws RemoteException
    {
        return actual.getName();
    }

    public String interpretLine( String line, Session session, Output out ) throws ShellException,
            RemoteException
    {
        return actual.interpretLine( line, session, out );
    }

    public Serializable interpretVariable( String key, Serializable value, Session session )
            throws ShellException, RemoteException
    {
        return actual.interpretVariable( key, value, session );
    }

    public String welcome() throws RemoteException
    {
        return actual.welcome();
    }

    public void setProperty( String key, Serializable value ) throws RemoteException
    {
        actual.setProperty( key, value );
    }

    public Serializable getProperty( String key ) throws RemoteException
    {
        return actual.getProperty( key );
    }

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

    public void makeRemotelyAvailable( int port, String name ) throws RemoteException
    {
        RmiLocation location = RmiLocation.location( "localhost", port, name );
        location.bind( this );
    }

    public String[] getAllAvailableCommands() throws RemoteException
    {
        return actual.getAllAvailableCommands();
    }

    public TabCompletion tabComplete( String partOfLine, Session session ) throws ShellException,
            RemoteException
    {
        return actual.tabComplete( partOfLine, session );
    }
}
