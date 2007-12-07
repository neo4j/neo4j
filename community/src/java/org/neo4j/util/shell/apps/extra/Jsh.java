package org.neo4j.util.shell.apps.extra;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Jsh extends AbstractApp
{
	public String execute( AppCommandParser parser, Session session, Output out )
	    throws ShellException
	{
		String line = parser.getLineWithoutCommand();
		new JshExecutor().execute( line, session, out );
		return null;
	}

	@Override
	public String getDescription()
	{
		return
			"Runs python (jython) scripts. Usage: jsh <python script line>\n" +
			"  Example: jsh --doSomething arg1 \"arg 2\" " +
			"--doSomethingElse arg1\n" +
			"Where the python scripts doSomething.py and " +
			"doSomethingElse.py exists in one of " +
			"environment variable\n" + JshExecutor.PATH_STRING +
			" paths (default is " + JshExecutor.DEFAULT_PATHS + ")";
	}
}
