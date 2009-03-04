package org.neo4j.util.shell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class StandardConsole implements Console
{
    private BufferedReader consoleReader;
    
	/**
	 * Prints a formatted string to the console (System.out).
	 * @param format the string/format to print.
	 * @param args values used in conjunction with {@code format}.
	 */
	public void format( String format, Object... args )
	{
		System.out.print( format );
	}
	
	/**
	 * @return the next line read from the console (user input).
	 */
	public String readLine()
	{
	    try
	    {
	        if ( consoleReader == null )
	        {
	            consoleReader = new BufferedReader( new InputStreamReader(
	                System.in ) );
	        }
	        return consoleReader.readLine();
	    }
	    catch ( IOException e )
	    {
	        throw new RuntimeException( e );
	    }
	}
}
