package org.neo4j.util.shell;

public interface Console
{
	/**
	 * Prints a formatted string to the console (System.out).
	 * @param format the string/format to print.
	 * @param args values used in conjunction with {@code format}.
	 */
	void format( String format, Object... args );
	
	/**
	 * @return the next line read from the console (user input).
	 */
	String readLine();
}
