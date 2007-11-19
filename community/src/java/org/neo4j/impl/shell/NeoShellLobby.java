/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.util.shell.AbstractServer;
import org.neo4j.util.shell.ShellException;
import org.neo4j.util.shell.ShellServer;

public class NeoShellLobby
{
	private static final NeoShellLobby INSTANCE = new NeoShellLobby();
	
	public static NeoShellLobby getInstance()
	{
		return INSTANCE;
	}
	
	private NeoShellLobby()
	{
	}
	
	public ShellServer startNeoShellServerWithNeo(
		Class<? extends RelationshipType> relTypes, String neoPath )
		throws ShellException
	{
		final NeoService neo = new EmbeddedNeo( relTypes, neoPath );
		try
		{
			final ShellServer server = new NeoShellServer( neo ); //, relTypes );
			Runtime.getRuntime().addShutdownHook( new Thread()
			{
				@Override
				public void run()
				{
					try
					{
						server.shutdown();
					}
					catch ( RemoteException e )
					{
						e.printStackTrace();
					}
					neo.shutdown();
				}
			} );
			server.makeRemotelyAvailable( AbstractServer.DEFAULT_PORT,
				AbstractServer.DEFAULT_NAME );
			return server;
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
	}
}
