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
package org.neo4j.shell.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.apps.Alias;

/**
 * A common implementation of an {@link AppShellServer}. The server can be given
 * one or more java packages f.ex. "org.neo4j.shell.apps" where some
 * common apps exist. All classes in those packages which implements the
 * {@link App} interface will be available to execute.
 */
public abstract class AbstractAppServer extends AbstractServer
	implements AppShellServer
{
	private Map<String, Class<? extends App>> apps =
	    new TreeMap<String, Class<? extends App>>();
	private Map<Class<? extends App>, App> appInstances =
	    new HashMap<Class<? extends App>, App>();

	/**
	 * Constructs a new server.
	 * @throws RemoteException if there's an RMI error.
	 */
	public AbstractAppServer()
		throws RemoteException
	{
		super();
	}

	public void addApp( Class<? extends App> appClass )
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
	    
	    App result = this.appInstances.get( app );
	    if ( result == null )
	    {
    	    try
            {
                result = app.newInstance();
                ( ( AbstractApp ) result ).setServer( this );
                this.appInstances.put( app, result );
            }
            catch ( Exception e )
            {
                // TODO It's OK, or is it?
            }
	    }
        return result;
	}
	
	@Override
	public void shutdown()
	{
	    for ( App app : this.appInstances.values() )
	    {
	        app.shutdown();
	    }
	    
	    super.shutdown();
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException
	{
		if ( line == null || line.trim().length() == 0 )
		{
			return "";
		}
		
        line = replaceAlias( line, session );
		AppCommandParser parser = new AppCommandParser( this, line );
		return parser.app().execute( parser, session, out );
	}
	
	protected String replaceAlias( String line, Session session )
	        throws ShellException
    {
	    boolean changed = true;
	    Set<String> appNames = new HashSet<String>();
	    while ( changed )
	    {
	        changed = false;
    	    String appName = AppCommandParser.parseOutAppName( line );
    	    String prefixedKey = Alias.ALIAS_PREFIX + appName;
    	    String alias = ( String ) AbstractApp.safeGet(
    	            session, prefixedKey );
    	    if ( alias != null && appNames.add( alias ) )
    	    {
    	        changed = true;
    	        line = alias + line.substring( appName.length() );
    	    }
	    }
	    return line;
    }

    @Override
	public Iterable<String> getAllAvailableCommands()
	{
		return new ArrayList<String>( apps.keySet() );
	}
}
