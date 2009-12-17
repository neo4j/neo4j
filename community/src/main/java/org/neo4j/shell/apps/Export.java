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
package org.neo4j.shell.apps;

import org.neo4j.shell.AbstractApp;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the Bash application "export" and uses the client session
 * {@link Session} as the data container.
 */
public class Export extends AbstractApp
{
	@Override
	public String getDescription()
	{
		return "Sets an environment variable. Usage: export <key>=<value>\n" +
			"F.ex: export NAME=\"Mattias Persson\"";
	}

	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		StringBuffer buffer = new StringBuffer();
		for ( String string : parser.arguments() )
		{
			buffer.append( string );
		}
		
		String string = buffer.toString();
		int index = string.indexOf( '=' );
		if ( index == -1 )
		{
			throw new ShellException( "Invalid format <key>=<value>" );
		}
		
		String key = string.substring( 0, index );
		String value = string.substring( index + 1 );
		if ( value == null || value.trim().length() == 0 )
		{
			safeRemove( session, key );
		}
		else
		{
			safeSet( session, key, value );
		}
		return null;
	}
}
