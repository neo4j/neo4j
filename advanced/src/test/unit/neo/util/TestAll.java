package unit.neo.util;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestAll extends TestSuite
{
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		suite.addTest( TestArrayMap.suite() );
		suite.addTest( TestArrayIntSet.suite() );
		return new TestSetup( suite );
	}
	
	public static void main( String args[] )
	{
		junit.textui.TestRunner.run( suite() );
	}
}