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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.neo4j.shell.Output;

/**
 * An implementation of {@link Output} optimized to use with a
 * {@link SameJvmClient}.
 */
public class SystemOutput implements Output
{
	private PrintWriter out;

    public SystemOutput()
    {
        this( System.out );
    }

    public SystemOutput( OutputStream out )
    {
        this.out = new PrintWriter( new OutputStreamWriter( out, StandardCharsets.UTF_8 ) );
    }

    public void print( Serializable object )
	{
		out.print(object);
	}
	
	public void println()
	{
		out.println();
        out.flush();
	}

	public void println( Serializable object )
	{
		out.println( object );
        out.flush();
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
