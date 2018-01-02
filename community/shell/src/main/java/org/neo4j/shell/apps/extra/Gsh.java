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
package org.neo4j.shell.apps.extra;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;

/**
 * A way to execute groovy scripts from the shell. It doesn't use the groovy
 * classes directly, but instead purely via reflections... This gives the
 * advantage of not being dependent on groovy at compile-time.
 * 
 * So if the groovy classes is in the classpath at run-time then groovy scripts
 * can be executed, otherwise it will say that "Groovy isn't available".
 * 
 * It has the old style script/argument format of:
 * sh$ gsh --script1 arg1 arg2 arg3 --script2 arg1 arg2
 * 
 * The paths to look for groovy scripts is decided by the environment variable
 * GSH_PATH, also there are some default paths: ".", "script", "src/script".
 */
public class Gsh extends AbstractApp
{
	public Continuation execute( AppCommandParser parser, Session session,
		Output out ) throws Exception
	{
		String line = parser.getLineWithoutApp();
		new GshExecutor().execute( line, session, out );
		return Continuation.INPUT_COMPLETE;
	}
	
	@Override
	public String getDescription()
	{
		GshExecutor anExecutor = new GshExecutor();
		return "Runs groovy scripts. Usage: gsh <groovy script line>\n" +
			"Example: gsh --doSomething arg1 \"arg 2\" " +
			"--doSomethingElse arg1\n\n" +
			"Groovy scripts doSomething.groovy and " +
			"doSomethingElse.groovy must exist " +
			"in one of environment variable " + anExecutor.getPathKey() +
			" paths (default is " + anExecutor.getDefaultPaths() + ")";
	}
}
