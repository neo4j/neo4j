package org.neo4j.util.shell;

/**
 * Mimics the java 1.6 Console class, but is supplied here for java version
 * prior to 1.6.
 */
public class Console
{
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
			StringBuffer text = new StringBuffer();
			while ( true )
			{
				int charRead = System.in.read();
				if ( charRead == '\r' || charRead == '\n' )
				{
					// Skip garbage chars.
					System.in.skip( System.in.available() );
					break;
				}
				
				text.append( ( char ) charRead );
			}
			return text.toString();
		}
		catch ( java.io.IOException e )
		{
			throw new RuntimeException( e );
		}
	}
}
