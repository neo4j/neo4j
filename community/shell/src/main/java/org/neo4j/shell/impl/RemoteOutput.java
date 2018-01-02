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
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.neo4j.shell.Output;

/**
 * An implementation of {@link Output} which outputs over RMI to from the
 * server to the client.
 */
public class RemoteOutput extends UnicastRemoteObject implements Output
{
	private RemoteOutput() throws RemoteException
	{
		super();
	}
	
	/**
	 * Convenient method for creating a new instance without having to catch
	 * the {@link RemoteException}
	 * @return a new {@link RemoteOutput} instance.
	 */
	public static RemoteOutput newOutput()
	{
		try
		{
			return new RemoteOutput();
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public void print( Serializable object )
	{
		System.out.print( object );
	}
	
	public void println()
	{
		System.out.println();
	}

	public void println( Serializable object )
	{
		System.out.println( object );
	}

	public Appendable append( char ch )
	{
		this.print( ch );
		return this;
	}
	
	/**
	 * A util method for getting the correct string for {@code sequence},
	 * see {@link Appendable}.
	 * @param sequence the string value.
	 * @return the correct string.
	 */
	public static String asString( CharSequence sequence )
	{
		return sequence == null ? "null" : sequence.toString();
	}

	public Appendable append( CharSequence sequence )
	{
		this.println( asString( sequence ) );
		return this;
	}

	public Appendable append( CharSequence sequence, int start, int end )
	{
		this.print( asString( sequence ).substring( start, end ) );
		return this;
	}
}
