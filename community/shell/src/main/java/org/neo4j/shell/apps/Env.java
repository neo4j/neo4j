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
package org.neo4j.shell.apps;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;

/**
 * Mimics the Bash application "env" and uses the client session {@link Session}
 * as the data container.
 */
@Service.Implementation( App.class )
public class Env extends AbstractApp
{
	@Override
	public String getDescription()
	{
		return "Lists all environment variables";
	}

	public Continuation execute( AppCommandParser parser, Session session,
		Output out ) throws Exception
	{
		for ( String key : session.keys() )
		{
			Object value = session.get( key );
			out.println( key + "=" + ( value == null ? "" : value ) );
		}
		return Continuation.INPUT_COMPLETE;
	}
}
