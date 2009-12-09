/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.shell.apps.extra;

import org.neo4j.shell.AbstractApp;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

public class Jsh extends AbstractApp
{
	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		String line = parser.getLineWithoutCommand();
		new JshExecutor().execute( line, session, out );
		return null;
	}

	@Override
	public String getDescription()
	{
		JshExecutor anExecutor = new JshExecutor();
		return
			"Runs python (jython) scripts. Usage: jsh <python script line>\n" +
			"Example: jsh --doSomething arg1 \"arg 2\" " +
			"--doSomethingElse arg1\n\n" +
			"Python scripts doSomething.py and doSomethingElse.py " +
			"must exist\n" +
			"in one of environment variable " + anExecutor.getPathKey() +
			" paths (default is " + anExecutor.getDefaultPaths() + ")";
	}
}
