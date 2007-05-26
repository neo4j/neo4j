package org.neo4j.util.shell;

/**
 * Completely server-side
 */
public interface App
{
	String getName();
	
	OptionValueType getOptionValueType( String option );
	
	String[] getAvailableOptions();
	
	String execute( CommandParser parser, Session session, Output out )
		throws ShellException;
	
	void setServer( ShellServer server );
	
	String getDescription();
	
	String getDescription( String option );
}
