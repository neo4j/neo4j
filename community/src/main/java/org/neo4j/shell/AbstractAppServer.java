/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.shell;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A common implementation of an {@link AppShellServer}. The server can be given
 * one or more java packages f.ex. "org.neo4j.util.shell.apps" where some
 * common apps exist. All classes in those packages which implements the
 * {@link App} interface will be available to execute.
 */
public abstract class AbstractAppServer extends AbstractServer
	implements AppShellServer
{
	private Map<String, Class<? extends App>> apps =
	    new TreeMap<String, Class<? extends App>>();

	/**
	 * Constructs a new server.
	 * @throws RemoteException if there's an RMI error.
	 */
	public AbstractAppServer()
		throws RemoteException
	{
		super();
	}

	protected void addApp( Class<? extends App> appClass )
	{
		this.apps.put( appClass.getSimpleName().toLowerCase(), appClass );
	}
	
	/**
	 * @return a list of the {@link App}s this server manages.
	 */
	public Set<Class<? extends App>> getApps()
	{
		return new TreeSet<Class<? extends App>>( this.apps.values() );
	}

	public App findApp( String command )
	{
	    Class<? extends App> app = this.apps.get( command );
	    if ( app == null )
	    {
	        return null;
	    }
	    
	    App result = null;
	    try
        {
            result = app.newInstance();
            ( ( AbstractApp ) result ).setServer( this );
        }
        catch ( Exception e )
        {
            // TODO It's OK, or is it?
        }
        return result;
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException
	{
		if ( line == null || line.trim().length() == 0 )
		{
			return "";
		}
		
		AppCommandParser parser = new AppCommandParser( this, line );
		return parser.app().execute( parser, session, out );
	}
	
	@Override
	public Iterable<String> getAllAvailableCommands()
	{
		return new ArrayList<String>( apps.keySet() );
	}
}
