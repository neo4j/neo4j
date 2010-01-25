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
package org.neo4j.shell;

/**
 * A completely server-side "application", sort of like a unix application
 * (just like "ls" or "cd"). The API lets you define expected arguments and
 * options which are required or optional.
 */
public interface App
{
	/**
	 * @return the name of the application. 
	 */
	String getName();
	
	/**
	 * @param option the name of the option. An option could be like this:
	 * "ls -l" where "l" is an option.
	 * @return the option context for {@code option}.
	 */
	OptionValueType getOptionValueType( String option );
	
	/**
	 * @return the available options.
	 */
	String[] getAvailableOptions();
	
	/**
	 * The actual code for the application.
	 * @param parser holds the options (w/ or w/o values) as well as arguments.  
	 * @param session the client session (sort of like the environment
	 * for the execution).
	 * @param out the output channel for the execution, just like System.out.
	 * @return the result of the execution. It is up to the client to interpret
	 * this string, one example is that all apps returns null and the "exit"
	 * app returns "e" so that the server interprets the "e" as a sign that
	 * it should exit. 
	 * @throws ShellException if the execution fails.
	 */
	String execute( AppCommandParser parser, Session session, Output out )
		throws ShellException;
	
	/**
	 * Returns the server this app runs in.
	 * @return the server this app runs in.
	 */
	AppShellServer getServer();
	
	/**
	 * @return a general description of this application.
	 */
	String getDescription();
	
	/**
	 * @param option the option to get the description for.
	 * @return a description of a certain option.
	 */
	String getDescription( String option );
}
