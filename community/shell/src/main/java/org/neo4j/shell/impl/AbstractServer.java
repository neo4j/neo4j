/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.TabCompletion;
import org.neo4j.shell.Welcome;

/**
 * A common implementation of a {@link ShellServer}.
 */
public abstract class AbstractServer implements ShellServer
{
    private ShellServer remoteEndPoint;
    
	/**
	 * The default RMI name for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final String DEFAULT_NAME = "shell";
	
	/**
	 * The default RMI port for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final int DEFAULT_PORT = 1337;
	
	private final Map<Serializable, Session> clientSessions = new ConcurrentHashMap<Serializable, Session>();
	
	private final AtomicInteger nextClientId = new AtomicInteger();
	
	/**
	 * Constructs a new server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public AbstractServer()
		throws RemoteException
	{
		super();
	}
	
	public String getName()
	{
		return DEFAULT_NAME;
	}

	@Override
	public Serializable interpretVariable( Serializable clientID, String key ) throws ShellException, RemoteException
	{
		return (Serializable) getClientSession( clientID ).get( key );
	}
	
	protected Serializable newClientId()
	{
	    return nextClientId.incrementAndGet();
	}

	@Override
	public Welcome welcome( Map<String, Serializable> initialSession ) throws RemoteException
	{
	    Serializable clientId = newClientId();
	    if ( clientSessions.containsKey( clientId ) )
	        throw new IllegalStateException( "Client " + clientId + " already initialized" );
	    Session session = newSession( clientId, initialSession );
        clientSessions.put( clientId, session );
		try
        {
            return new Welcome( getWelcomeMessage(), clientId, getPrompt( session ) );
        }
        catch ( ShellException e )
        {
            throw new RemoteException( e.getMessage() );
        }
	}
	
	private Session newSession( Serializable id, Map<String, Serializable> initialSession )
    {
	    Session session = new Session( id );
	    initialPopulateSession( session );
	    for ( Map.Entry<String, Serializable> entry : initialSession.entrySet() )
	        session.set( entry.getKey(), entry.getValue() );
	    return session;
    }

    protected void initialPopulateSession( Session session )
    {
    }

    protected String getPrompt( Session session ) throws ShellException
    {
	    return "sh$ ";
    }

    protected String getWelcomeMessage()
    {
	    return "Welcome to the shell";
    }
    
    public Session getClientSession( Serializable clientID )
    {
        Session session = clientSessions.get( clientID );
        if ( session == null )
            throw new IllegalStateException( "Client " + clientID + " not initialized" );
        return session;
    }
    
    @Override
    public void leave( Serializable clientID ) throws RemoteException
    {
        // TODO how about clients not properly leaving?
        
        if ( clientSessions.remove( clientID ) == null )
            throw new IllegalStateException( "Client " + clientID + " not initialized" );
    }

    public synchronized void shutdown() throws RemoteException
	{
	    if ( remoteEndPoint != null )
	    {
	        remoteEndPoint.shutdown();
	        remoteEndPoint = null;
	    }
	}

	public synchronized void makeRemotelyAvailable( int port, String name )
		throws RemoteException
	{
	    if ( remoteEndPoint == null )
	        remoteEndPoint = new RemotelyAvailableServer( this );
	    remoteEndPoint.makeRemotelyAvailable( port, name );
	}
	
	public String[] getAllAvailableCommands()
	{
		return new String[0];
	}
	
	public TabCompletion tabComplete( String partOfLine, Session session )
	        throws ShellException, RemoteException
	{
	    return new TabCompletion( Collections.<String>emptyList(), 0 );
	}
	
	@Override
	public void setSessionVariable( Serializable clientID, String key, Object value )
	{
	    getClientSession( clientID ).set( key, value );
	}
}
