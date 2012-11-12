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
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.Session;

public class SessionImpl implements Session
{
    private static final long serialVersionUID = 1L;
    
    interface SessionWriter extends Remote
    {
        void set( String key, Serializable value ) throws RemoteException;
    }

    private static class SessionWriterImpl extends UnicastRemoteObject implements SessionWriter
    {
        private static final long serialVersionUID = 1L;
        
        private final Map<String, Serializable> properties;

        SessionWriterImpl( Map<String, Serializable> properties ) throws RemoteException
        {
            super();
            this.properties = properties;
        }

        public void set( String key, Serializable value )
        {
            setProperty( this.properties, key, value );
        }
    }

    private final Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
    final SessionWriter writer;

    public SessionImpl()
    {
        SessionWriter writerImpl;
        try
        {
            writerImpl = new SessionWriterImpl( properties );
        }
        catch ( RemoteException e )
        {
            writerImpl = null;
        }
        this.writer = writerImpl;
    }

	public void set( String key, Serializable value )
	{
        try
        {
            if ( writer != null ) writer.set( key, value );
        }
        catch ( RemoteException swallowed )
        {
        }
        setProperty( properties, key, value );
	}

    private static void setProperty( Map<String, Serializable> properties, String key,
            Serializable value )
    {
        if ( value == null )
        {
            properties.remove( key );
        }
        else
        {
            properties.put( key, value );
        }
    }

	public Serializable get( String key )
	{
		return this.properties.get( key );
	}

	public Serializable remove( String key )
	{
		return this.properties.remove( key );
	}

	public String[] keys()
	{
		return this.properties.keySet().toArray(
			new String[ this.properties.size() ] );
	}

	public Map<String, Serializable> asMap()
	{
	    return new HashMap<String, Serializable>( this.properties );
	}
}
