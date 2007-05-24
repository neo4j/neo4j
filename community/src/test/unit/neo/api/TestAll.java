package unit.neo.api;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestAll extends TestSuite
{
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		suite.addTest( TestNode.suite() );
		suite.addTest( TestRelationship.suite() );
		suite.addTest( TestNeo.suite() );
		suite.addTest( TestNeoCacheAndPersistence.suite() );
		suite.addTest( TestTraversal.suite() );
		suite.addTest( TestNodeSorting.suite() );
		suite.addTest( TestNeoConstrains.suite() );
		suite.addTest( TestPropertyTypes.suite() );
		return new TestSetup( suite );
	}
	
	public static void main( String args[] )
	{
		junit.textui.TestRunner.run( suite() );
	}
}