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

import org.neo4j.shell.apps.extra.JshExecutor;

/**
 * A common {@link ShellServer} implementation which is specialized in just
 * executing groovy scripts.
 */
public class JshServer extends AbstractServer
{
	/**
	 * Constructs a new groovy shell server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public JshServer() throws RemoteException
	{
		super();
		this.setProperty( AbstractClient.PROMPT_KEY, "jsh$ " );
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		session.set( AbstractClient.STACKTRACES_KEY, true );
		JshExecutor jsh = new JshExecutor();
		jsh.execute( line, session, out );
		return null;
	}
}
