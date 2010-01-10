/*
 * Copyright (c) 2008-2009 "Neo Technology,"
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
package org.neo4j.remote.transports;

import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.RemoteConnection;

/**
 * A {@link ConnectionTarget} that uses RMI for communication. Connecting to an RMI
 * site is as simple as creating a new instance of this class. To expose a
 * {@link GraphDatabaseService} for remote connections via RMI, use the static
 * {@link #register(BasicGraphDatabaseServer, String)} method of this class.
 * @author Tobias Ivarsson
 */
final class RmiTarget implements ConnectionTarget
{
    private final URI uri;

    /**
     * Creates a new {@link ConnectionTarget} that uses RMI for its communication.
     * @param resourceUri
     *            the RMI resource name in URL form.
     */
    public RmiTarget( URI resourceUri )
    {
        uri = resourceUri;
    }

    private RmiRemoteTarget site()
    {
        try
        {
            return ( RmiRemoteTarget ) Naming.lookup( uri.toString() );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
        catch ( NotBoundException e )
        {
            throw new RuntimeException(
                "Could not connect to the RMI site at \"" + uri + "\"", e );
        }
    }

    public RemoteConnection connect()
    {
        RmiRemoteTarget site = site();
        try
        {
            RmiConnection rmic = site.connect();
            return new RmiConnectionAdapter( rmic );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Could not initiate connection.", e );
        }
    }

    public RemoteConnection connect( String username, String password )
    {
        RmiRemoteTarget site = site();
        try
        {
            RmiConnection rmic = site.connect( username, password );
            return new RmiConnectionAdapter( rmic );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Could not initiate connection.", e );
        }
    }
}
