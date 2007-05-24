package unit.neo.transaction;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestAll extends TestSuite
{
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		suite.addTest( TestRWLock.suite() );
		suite.addTest( TestDeadlockDetection.suite() );
		suite.addTest( TestJtaCompliance.suite() );
		suite.addTest( TestTxLog.suite() );
		suite.addTest( TestTxEvents.suite() );
		suite.addTest( TestXaFramework.suite() );
		return new TestSetup( suite );
	}
	
	public static void main( String args[] )
	{
		junit.textui.TestRunner.run( suite() );
	}
}