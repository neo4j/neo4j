package org.neo4j.util.shell;

public class Console
{
	public void format( String format, Object... args )
	{
		System.out.print( format );
	}
	
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
