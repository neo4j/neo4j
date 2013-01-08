/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;

import org.neo4j.shell.Console;
import org.neo4j.shell.ShellClient;

/**
 * Implements the {@link Console} interface with jLine using reflections only,
 * since we don't wan't a runtime dependency on jLine.
 */
public class JLineConsole implements Console
{
	private Object consoleReader;
	
	static JLineConsole newConsoleOrNullIfNotFound( ShellClient client )
	{
		try
		{
			Object consoleReader =
				Class.forName( "jline.ConsoleReader" ).newInstance();
			consoleReader.getClass().getMethod( "setBellEnabled",
				Boolean.TYPE ).invoke( consoleReader, false );
			consoleReader.getClass().getMethod( "setBellEnabled", Boolean.TYPE ).invoke(
			        consoleReader, false );
            consoleReader.getClass().getMethod( "setUseHistory", Boolean.TYPE ).invoke(
                    consoleReader, true );

	        Object completor = Class.forName( JLineConsole.class.getPackage().getName() + "." +
	                "ShellTabCompletor" ).getConstructor( ShellClient.class ).newInstance( client );
            Class<?> completorClass = Class.forName( "jline.Completor" );
	        consoleReader.getClass().getMethod( "addCompletor",
	            completorClass ).invoke( consoleReader, completor );
	        
            Class<?> historyClass = Class.forName( "jline.History" );
            Object history = historyClass.newInstance();
            history.getClass().getMethod( "setHistoryFile", File.class ).invoke( history,
                    new File( ".shell_history" ) );
            consoleReader.getClass().getMethod( "setHistory", historyClass ).invoke(
                    consoleReader, history );

			return new JLineConsole( consoleReader, client );
		}
		catch ( Exception e )
		{
			return null;
		}
	}
	
	private JLineConsole( Object consoleReader, ShellClient client )
	{
		this.consoleReader = consoleReader;
	}
	
	public void format( String format, Object... args )
	{
		System.out.print( format );
	}
	
	public String readLine( String prompt )
	{
		try
		{
		    consoleReader.getClass().getMethod( "setDefaultPrompt", String.class ).invoke(
		            consoleReader, prompt );
			return ( String ) consoleReader.getClass().getMethod( "readLine" ).
				invoke( consoleReader );
		}
		catch ( RuntimeException e )
		{
		    throw e;
		}
		catch ( Exception e )
		{
			throw new RuntimeException( e );
		}
	}
}
