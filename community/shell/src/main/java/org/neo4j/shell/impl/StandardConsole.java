/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.neo4j.shell.Console;

/**
 * Uses System.in and System.out
 */
public class StandardConsole implements Console
{
    private BufferedReader consoleReader;
    
	/**
	 * Prints a formatted string to the console (System.out).
	 * @param format the string/format to print.
	 * @param args values used in conjunction with {@code format}.
	 */
	public void format( String format, Object... args )
	{
		System.out.print( format );
	}
	
	/**
	 * @return the next line read from the console (user input).
	 */
	public String readLine( String prompt )
	{
	    try
	    {
	        format( prompt );
	        if ( consoleReader == null )
	        {
	            consoleReader = new BufferedReader( new InputStreamReader(
	                System.in, "UTF-8" ) );
	        }
	        return consoleReader.readLine();
	    }
	    catch ( IOException e )
	    {
	        throw new RuntimeException( e );
	    }
	}
}
