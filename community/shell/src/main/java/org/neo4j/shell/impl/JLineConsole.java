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

import java.io.File;

import org.neo4j.shell.Console;
import org.neo4j.shell.ShellClient;

/**
 * Implements the {@link Console} interface with jLine using reflections only,
 * since we don't wan't a runtime dependency on jLine.
 */
public class JLineConsole implements Console
{
	private final Object consoleReader;

	static JLineConsole newConsoleOrNullIfNotFound( ShellClient client )
	{
		try
		{
			Object consoleReader =
				Class.forName( "jline.console.ConsoleReader" ).newInstance();
			consoleReader.getClass().getMethod( "setBellEnabled",
				Boolean.TYPE ).invoke( consoleReader, false );
			consoleReader.getClass().getMethod( "setExpandEvents", Boolean.TYPE ).invoke(
			        consoleReader, false );
            consoleReader.getClass().getMethod( "setHistoryEnabled", Boolean.TYPE ).invoke(
                    consoleReader, true );

	        Object completer = Class.forName( JLineConsole.class.getPackage().getName() + "." +
	                "ShellTabCompleter" ).getConstructor( ShellClient.class ).newInstance( client );
            Class<?> completerClass = Class.forName( "jline.console.completer.Completer" );
	        consoleReader.getClass().getMethod( "addCompleter",
	            completerClass ).invoke( consoleReader, completer );

            Class<?> historyClass = Class.forName( "jline.console.history.History" );
	        Object history = Class.forName( "jline.console.history.FileHistory" ).getConstructor(
	                File.class ).newInstance( new File( System.getProperty(
	                        "user.home" ), ".neo4j_shell_history" ) );
            consoleReader.getClass().getMethod( "setHistory", historyClass ).invoke( consoleReader, history );

			return new JLineConsole( consoleReader, client );
		}
		catch ( RuntimeException e )
		{
            // Only checked exceptions should cause us to return null,
            // a runtime exception is not expected - it could be an OOM
            // for instance, throw instead.
			throw e;
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

	@Override
    public void format( String format, Object... args )
	{
		System.out.print( format );
	}

	@Override
    public String readLine( String prompt )
	{
		try
		{
		    consoleReader.getClass().getMethod( "setPrompt", String.class ).invoke(
		            consoleReader, prompt );
		    Class<?> fileHistoryClass = Class.forName( "jline.console.history.FileHistory" );
		    Object history = consoleReader.getClass().getMethod( "getHistory" ).invoke(
		            consoleReader );
		    Class.forName( "jline.console.history.FileHistory" ).getMethod( "flush" ).invoke(
		            history );
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
