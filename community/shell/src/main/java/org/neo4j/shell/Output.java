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

/**
 * An interface for printing output, like System.out, The implementation can
 * be via RMI or locally. 
 */
public interface Output extends Appendable, Remote
{
	/**
	 * Prints a line to the output.
	 * @param object the object to print (the string representation of it).
	 * @throws RemoteException RMI error.
	 */
	void print( Serializable object ) throws RemoteException;
	
	/**
	 * Prints a new line to the output.
	 * @throws RemoteException RMI error.
	 */
	void println() throws RemoteException;
	
	/**
	 * Prints a line with new line to the output.
	 * @param object the object to print (the string representation of it).
	 * @throws RemoteException RMI error.
	 */
	void println( Serializable object ) throws RemoteException;
}
