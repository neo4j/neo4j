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

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A shell server which clients can connect to and send requests
 * (command lines).
 */
public interface ShellServer extends Remote
{
	/**
	 * @return the name of this server.
	 * @throws RemoteException RMI error.
	 */
	String getName() throws RemoteException;
	
	/**
	 * Receives a command line (probably from a {@link ShellClient}) and reacts
	 * to it. Output is written to the {@link Output} object.
	 * @param line the command line to react to.
	 * @param session the client session (environment).
	 * @param out where output should go (like System.out).
	 * @return some result from the execution, it's up to the client to
	 * interpret the result, if any. F.ex. "e" could mean that the client
	 * should exit, in response to a request "exit". 
	 * @throws ShellException if there was an error in the
	 * interpretation/execution of the command line.
	 * @throws RemoteException RMI error.
	 */
	String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException;
	
	/**
	 * Interprets a variable from a client session and returns the
	 * interpreted result.
	 * @param key the variable key.
	 * @param value the variable value.
	 * @param session the client session to get necessary values from to
	 * help the interpretation.
	 * @return the interpreted value.
	 * @throws ShellException if some error should occur.
	 * @throws RemoteException RMI error.
	 */
	Serializable interpretVariable( String key, Serializable value,
		Session session ) throws ShellException, RemoteException;
 
	/**
	 * @return a nice welcome for a client. Typically a client connects and
	 * asks for a greeting message to display to the user.
	 * @throws RemoteException RMI error.
	 */
	String welcome() throws RemoteException;
	
	/**
	 * Sets a server property, typically used for setting default values which
	 * clients can read at startup, but can be used for anything.
	 * @param key the property key.
	 * @param value the property value.
	 * @throws RemoteException RMI error.
	 */
	void setProperty( String key, Serializable value ) throws RemoteException;
	
	/**
	 * @param key the property key.
	 * @return the server property value for {@code key}.
	 * @throws RemoteException RMI error.
	 */
	Serializable getProperty( String key ) throws RemoteException;
	
	/**
	 * Shuts down the server.
	 * @throws RemoteException RMI error.
	 */
	void shutdown() throws RemoteException;
	
	/**
	 * Makes this server available for clients to connect to via RMI.
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws RemoteException RMI error.
	 */
	void makeRemotelyAvailable( int port, String name ) throws RemoteException;
	
	/**
	 * @return all the available commands one can issue to this server.
	 * @throws RemoteException RMI error.
	 */
	String[] getAllAvailableCommands() throws RemoteException;
	
	/**
	 * Tries to complete a half-entered line and returns possible candidates,
	 * in the form of a {@link TabCompletion}.
	 * @param partOfLine the half-entered line to try to complete.
	 * @param session the client {@link Session}.
	 * @return a {@link TabCompletion} containing the possible candidates for completion.
     * @throws ShellException if some error should occur.
     * @throws RemoteException RMI error.
	 */
	TabCompletion tabComplete( String partOfLine, Session session )
	        throws ShellException, RemoteException;
}
