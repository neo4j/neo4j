package org.neo4j.util.shell;

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
	 * Sets which server to run in.
	 * @param server the server to run in.
	 */
	void setServer( AppShellServer server );
	
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
