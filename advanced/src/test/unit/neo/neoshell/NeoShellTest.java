package unit.neo.neoshell;

import junit.framework.TestCase;
import org.neo4j.impl.shell.NeoShellServer;
import org.neo4j.util.shell.AppCommandParser;

public class NeoShellTest extends TestCase
{
	private AppCommandParser parse( String line ) throws Exception
	{
		return new AppCommandParser( new NeoShellServer( null, null ), line );
	}
	
	public void testParserEasy() throws Exception
	{
		AppCommandParser parser = this.parse( "ls -la" );
		assertEquals( "ls", parser.getAppName() );
		assertEquals( 2, parser.options().size() );
		assertTrue( parser.options().containsKey( "l" ) );
		assertTrue( parser.options().containsKey( "a" ) );
		assertTrue( parser.arguments().isEmpty() );
	}
	
	public void testParserArguments() throws Exception
	{
		AppCommandParser parser =
			this.parse( "set -t java.lang.Integer key value" );
		assertEquals( "set", parser.getAppName() );
		assertTrue( parser.options().containsKey( "t" ) );
		assertEquals( "java.lang.Integer", parser.options().get( "t" ) );
		assertEquals( 2, parser.arguments().size() );
		assertEquals( "key", parser.arguments().get( 0 ) );
		assertEquals( "value", parser.arguments().get( 1 ) );
		
		assertException( "set -tsd" );
	}
	
	private void assertException( String command )
	{
		try
		{
			this.parse( command );
			fail( "Should fail" );
		}
		catch ( Exception e )
		{
			// Good
		}
	}
}
