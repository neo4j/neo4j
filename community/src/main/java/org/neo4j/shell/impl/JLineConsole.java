/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.shell.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import jline.ConsoleReader;
import jline.History;

import org.neo4j.shell.Console;
import org.neo4j.shell.ShellClient;

/**
 * Implements the {@link Console} interface with jLine using reflections only.
 */
public class JLineConsole implements Console
{
	private ShellClient client;
	private Object consoleReader;
	private Object completor;
	private boolean successfulGrabAvailableCommands = true;
	
	static JLineConsole newConsoleOrNullIfNotFound( ShellClient client )
	{
		try
		{
			Object consoleReader =
				Class.forName( "jline.ConsoleReader" ).newInstance();
			consoleReader.getClass().getMethod( "setBellEnabled",
				Boolean.TYPE ).invoke( consoleReader, false );
			ConsoleReader reader = (ConsoleReader) consoleReader;
			reader.setBellEnabled(false);
	        reader.setUseHistory(true);

	        try {
	            History history = new History();
	            history.setHistoryFile(new File(".shell_history"));
	            reader.setHistory(history);
	        } catch (IOException e) {
	        }

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
		this.client = client;
	}
	
	public void format( String format, Object... args )
	{
		System.out.print( format );
	}
	
	private void grabAvailableCommands() throws Exception
	{
		Class<?> completorClass = Class.forName( "jline.Completor" );
		if ( completor != null )
		{
			consoleReader.getClass().getMethod( "removeCompletor",
				completorClass ).invoke( consoleReader, completor );
			completor = null;
		}
		
		ArrayList<String> commandList = new ArrayList<String>();
		for ( String command :
			client.getServer().getAllAvailableCommands() )
		{
			commandList.add( command );
		}
		Object commandsArray = Array.newInstance( String.class,
			commandList.size() );
		int counter = 0;
		for ( String command : commandList )
		{
			Array.set( commandsArray, counter++, command );
		}
		
		completor = Class.forName( "jline.SimpleCompletor" ).
		getConstructor( commandsArray.getClass() ).newInstance(
			commandsArray );
		consoleReader.getClass().getMethod( "addCompletor",
			completorClass ).invoke( consoleReader, completor );
	}
	
	public String readLine()
	{
	    try
	    {
	        grabAvailableCommands();
	        successfulGrabAvailableCommands = true;
	    }
	    catch ( Exception e )
	    {
            // It's ok. Maybe there's problems with the server connection?
	        if ( successfulGrabAvailableCommands )
	        {
	            System.err.println( "Couldn't grab available commands: " +
	                e.toString() );
	            successfulGrabAvailableCommands = false;
	        }
	    }
        
		try
		{
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
