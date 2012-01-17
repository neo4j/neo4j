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
package org.neo4j.shell.impl;

import java.io.Serializable;

import org.neo4j.shell.Output;

/**
 * An implementation of {@link Output} optimized to use with a
 * {@link SameJvmClient}.
 */
public class SystemOutput implements Output
{
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

	public Appendable append( CharSequence sequence )
	{
		this.println( RemoteOutput.asString( sequence ) );
		return this;
	}

	public Appendable append( CharSequence sequence, int start, int end )
	{
		this.print( RemoteOutput.asString( sequence ).substring( start, end ) );
		return this;
	}
}
