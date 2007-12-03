package org.neo4j.util.shell.apps.extra;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

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
	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		String line = parser.getLineWithoutCommand();
		new GshExecutor().execute( line, session, out );
		return null;
	}
	
	@Override
	public String getDescription()
	{
		return "Runs groovy scripts. Usage: gsh <groovy script line>\n" +
			"  Example: gsh --doSomething arg1 \"arg 2\" " +
			"--doSomethingElse arg1\n" +
			"Where the groovy script doSomething.groovy and " +
			"doSomethingElse.groovy exists in one of " +
			"environment variable\n" + GshExecutor.PATH_STRING +
			" paths (default is " + GshExecutor.DEFAULT_PATHS + ")";
	}
}
