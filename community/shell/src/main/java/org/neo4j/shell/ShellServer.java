/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Map;

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
     * @param clientID identifying the client.
	 * @param line the command line to react to.
	 * @param out where output should go (like System.out).
	 * @return some result from the execution, it's up to the client to
	 * interpret the result, if any. F.ex. "e" could mean that the client
	 * should exit, in response to a request "exit".
	 * @throws ShellException if there was an error in the
	 * interpretation/execution of the command line.
	 * @throws RemoteException RMI error.
	 */
	Response interpretLine( Serializable clientID, String line, Output out )
		throws ShellException, RemoteException;

	/**
	 * Interprets a variable from a client session and returns the
	 * interpreted result.
	 * @param clientID identifying the client.
	 * @param key the variable key.
	 * help the interpretation.
	 * @return the interpreted value.
	 * @throws ShellException if some error should occur.
	 * @throws RemoteException RMI error.
	 */
	Serializable interpretVariable( Serializable clientID, String key ) throws ShellException, RemoteException;

    /**
     * Marks the client's active transaction as terminated.
     * @param clientID identifying the client.
     * @throws RemoteException RMI error.
     */
    public void terminate( Serializable clientID ) throws RemoteException;

    /**
     * @param initialSession the initial session variables that the client would
     * like to override or add to any initial server session variables.
     * @return a nice welcome for a client. Typically a client connects and
     * asks for a greeting message to display to the user.
     * @throws RemoteException RMI error.
     * @throws ShellException wraps general errors
     */
	Welcome welcome( Map<String, Serializable> initialSession ) throws RemoteException, ShellException;

	/**
	 * Notifies this server that the client identified by {@code clientID} is about to
	 * leave, so any session associated with it will be removed.
	 * @param clientID the ID which identifies the client which is leaving.
	 * These IDs are handed out from {@link #welcome(Map)}.
	 * @throws RemoteException RMI error.
	 */
	void leave( Serializable clientID ) throws RemoteException;

	/**
	 * Shuts down the server.
	 * @throws RemoteException RMI error.
	 */
	void shutdown() throws RemoteException;

	/**
	 * Makes this server available at {@code localhost} for clients to connect to via RMI.
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws RemoteException RMI error.
	 */
	void makeRemotelyAvailable( int port, String name ) throws RemoteException;

	/**
	 * Makes this server available at the specific {@code host} for clients to connect to via RMI.
	 * @param host the host to make this server available at.
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws RemoteException RMI error.
	 */
	void makeRemotelyAvailable( String host, int port, String name ) throws RemoteException;

	/**
	 * @return all the available commands one can issue to this server.
	 * @throws RemoteException RMI error.
	 */
	String[] getAllAvailableCommands() throws RemoteException;

	/**
	 * Tries to complete a half-entered line and returns possible candidates,
	 * in the form of a {@link TabCompletion}.
     * @param clientID identifying the client.
	 * @param partOfLine the half-entered line to try to complete.
	 * @return a {@link TabCompletion} containing the possible candidates for completion.
     * @throws ShellException if some error should occur.
     * @throws RemoteException RMI error.
	 */
	TabCompletion tabComplete( Serializable clientID, String partOfLine )
	        throws ShellException, RemoteException;

	/**
	 * Sets a session property for the session identified by {@code clientID}.
	 * @param clientID the client ID to identify the session.
	 * @param key the property key.
	 * @param value the property value.
     * @throws RemoteException RMI error.
     * @throws ShellException if an error occurs during initialization.
	 */
	void setSessionVariable( Serializable clientID, String key, Object value ) throws RemoteException, ShellException;
}
