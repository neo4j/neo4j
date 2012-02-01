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
package org.neo4j.shell;

/**
 * Represents a shell client which communicates with a {@link ShellServer}.
 * A client is very thin, it just grabs a command line from the user and sends
 * it to the server, letting everything happen server-side.
 */
public interface ShellClient
{
	/**
	 * Grabs the console prompt.
	 */
	void grabPrompt();
	
	/**
	 * Reads the next line from the user console.
	 * @param prompt the prompt to display.
	 * @return the next command line from the user.
	 */
	String readLine( String prompt );
	
	/**
	 * @return the session (or environment) for this client.
	 */
	Session session();
	
	/**
	 * @return the server to communicate with.
	 */
	ShellServer getServer();
	
	/**
	 * @return the output instance where output will be passed to.
	 */
	Output getOutput();
	
	/**
	 * @return the time (millis) when the most recent connection was made
	 * to the master.
	 */
	long timeForMostRecentConnection();
	
	/**
	 * Shuts down any resource needing to shut down.
	 */
	void shutdown();
}
