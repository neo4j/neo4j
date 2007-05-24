package unit.neo.event;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.neo4j.impl.event.EventModule;

public class TestAll extends TestSuite
{
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		suite.addTest( TestEventManager.suite() );
		return new TestSetup( suite );
	}
	
	public static void main( String args[] )
	{
		EventModule evtModule = new EventModule();
		evtModule.init();
		evtModule.start();
		junit.textui.TestRunner.run( suite() );
		evtModule.stop();
		evtModule.destroy();
	}
}