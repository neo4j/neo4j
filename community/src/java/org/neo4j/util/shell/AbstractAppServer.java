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
package org.neo4j.util.shell;

import java.lang.reflect.Modifier;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
	private Set<String> packages = new HashSet<String>();
//	private Set<String> availableCommands;

	/**
	 * Constructs a new server.
	 * @throws RemoteException if there's an RMI error.
	 */
	public AbstractAppServer()
		throws RemoteException
	{
		super();
	}

	/**
	 * Adds a package to scan for apps.
	 * @param pkg the java package, f.ex. "org.neo4j.util.shell.apps".
	 */
	public void addPackage( String pkg )
	{
		this.packages.add( pkg );
	}
	
	/**
	 * @return packages added by {@link #addPackage(String)}.
	 */
	public Set<String> getPackages()
	{
		return new HashSet<String>( packages );
	}

	public App findApp( String command )
	{
		for ( String pkg : this.packages )
		{
			String name = pkg + "." +
				command.substring( 0, 1 ).toUpperCase() +
				command.substring( 1, command.length() ).toLowerCase();
			try
			{
				Class<?> cls = Class.forName( name );
				if ( !cls.isInterface() && App.class.isAssignableFrom( cls ) &&
					!Modifier.isAbstract( cls.getModifiers() ) )
				{
					App theApp = ( App ) cls.newInstance();
					theApp.setServer( this );
					return theApp;
				}
			}
			catch ( Exception e )
			{
			}
			catch ( NoClassDefFoundError e )
			{
				// Well, if you at the prompt hit the name 'nodeorrelationship'
				// f.ex. then a NoClassDefFoundError will be thrown since
				// there exists a class by that name, but the case is wrong.
				// In this particular case the class is NodeOrRelationship.
			}
		}
		return null;
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
		return findAllApps();
	}
	
	protected Set<String> findAllApps()
	{
		Collection<? extends Class<?>> apps = ClassLister.
			listClassesExtendingOrImplementing( App.class, this.packages );
		Set<String> set = new TreeSet<String>();
		for ( Class<?> app : apps )
		{
			set.add( app.getSimpleName().toLowerCase() );
		}
		return set;
	}
}
