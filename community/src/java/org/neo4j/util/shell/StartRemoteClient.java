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

/**
 * Convenience main class for starting a client which connects to a remote
 * server. Which port/name to connect to may be specified as arguments.
 */
public class StartRemoteClient extends AbstractStarter
{
	/**
	 * Starts a client and connects to a remote server.
	 * @param args may contain RMI port/name to the server.
	 */
	public static void main( String[] args )
	{
		try
		{
			printGreeting( args );
			ShellClient client = ShellLobby.newClient(
			    getPort( args ), getShellName( args ) );
			setSessionVariablesFromArgs( client, args );
			client.grabPrompt();
		}
		catch ( Exception e )
		{
			System.err.println( "Can't start client shell: " + e );
			System.exit( 1 );
		}
	}
	
	private static void printGreeting( String[] args )
	{
		if ( args.length == 0 )
		{
			System.out.println( "NOTE: No port or RMI name specified, using " +
				"default port " + AbstractServer.DEFAULT_PORT + " and name '" +
				AbstractServer.DEFAULT_NAME + "'." );
		}
	}
	
	private static String getArg( String[] args, int index )
	{
	    int counter = 0;
	    for ( String arg : args )
	    {
	        if ( !arg.startsWith( "-" ) )
	        {
	            if ( counter == index )
	            {
	                return arg;
	            }
	            counter++;
	        }
	    }
	    throw new ArrayIndexOutOfBoundsException();
	}
	
	private static int getPort( String[] args )
	{
		try
		{
		    String arg = getArg( args, 0 );
			return arg != null ? Integer.parseInt( arg ) :
				AbstractServer.DEFAULT_PORT;
		}
		catch ( ArrayIndexOutOfBoundsException e )
		// Intentionally let NumberFormat propagate out to user
		{
			return AbstractServer.DEFAULT_PORT;
		}
	}
	
	private static String getShellName( String[] args )
	{
		try
		{
		    String arg = getArg( args, 1 );
			return arg != null ? arg : AbstractServer.DEFAULT_NAME;
		}
		catch ( ArrayIndexOutOfBoundsException e )
		{
			return AbstractServer.DEFAULT_NAME;
		}
	}
}
