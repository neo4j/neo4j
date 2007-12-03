/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.shell.apps;

import org.neo4j.api.core.Node;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. renames a property.
 * It could also (regarding POSIX) move nodes, but it doesn't).
 */
public class Mv extends NeoApp
{
	/**
	 * Constructs a new "mv" application.
	 */
	public Mv()
	{
		this.addValueType( "o", new OptionContext( OptionValueType.NONE,
			"To override if the key already exists" ) );
	}
	
	@Override
	public String getDescription()
	{
		return "Renames a property. Usage: mv <key> <new-key>";
	}

	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException
	{
		if ( parser.arguments().size() != 2 )
		{
			throw new ShellException(
				"Must supply <from-key> <to-key> arguments" );
		}
		String fromKey = parser.arguments().get( 0 );
		String toKey = parser.arguments().get( 1 );
		boolean mayOverwrite = parser.options().containsKey( "o" );
		Node currentNode = this.getCurrentNode( session );
		if ( !currentNode.hasProperty( fromKey ) )
		{
			throw new ShellException( "Property '" + fromKey +
				"' doesn't exist" );
		}
		if ( currentNode.hasProperty( toKey ) )
		{
			if ( !mayOverwrite )
			{
				throw new ShellException( "Property '" + toKey +
					"' already exists, supply -o flag to overwrite" );
			}
			else
			{
				currentNode.removeProperty( toKey );
			}
		}
		
		Object value = currentNode.removeProperty( fromKey );
		currentNode.setProperty( toKey, value );
		return null;
	}
}
