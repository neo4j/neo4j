package unit.neo.cache;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestAll extends TestSuite
{
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		suite.addTest( TestFifoCache.suite() );
		suite.addTest( TestLruCache.suite() );
		return new TestSetup( suite );
	}
	
	public static void main( String args[] )
	{
		junit.textui.TestRunner.run( suite() );
	}
}
