package unit.neo;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.neo4j.api.core.EmbeddedNeo;

public class TestAll extends TestSuite
{
	// Returns the generates suite
	public static Test suite()
	{
		return new TestSetup( generateSuite() );
	}
	
	// Generates a test suite
	private static TestSuite generateSuite()
	{
		// ADD YOUR NEW TESTS HERE:
		TestSuite suite = new TestSuite();
		suite.addTest( unit.neo.cache.TestAll.suite() ); 
		suite.addTest( unit.neo.event.TestAll.suite() );
		suite.addTest( unit.neo.api.TestAll.suite() );
		suite.addTest( unit.neo.transaction.TestAll.suite() );
		suite.addTest( unit.neo.store.TestAll.suite() );
		return suite;
	}
	
	private static EmbeddedNeo neo;
	
	// Starts up the kernel
	private static void startupKernel()
	{
		neo = new EmbeddedNeo( MyRelTypes.class, "var/nioneo", true );
	}
	
	// Shuts down the kernel
	private static void shutdownKernel()
	{
		neo.shutdown();
	}
	
	public static void main( String args[] )
	{
		startupKernel();
		junit.textui.TestRunner.run( suite() );
		shutdownKernel();
		org.neo4j.impl.command.CommandManager.getManager().dumpStack();		
		org.neo4j.impl.transaction.LockManager.getManager().dumpRagStack();
		org.neo4j.impl.transaction.LockManager.getManager().dumpAllLocks();
		( ( org.neo4j.impl.transaction.TxManager ) 
			org.neo4j.impl.transaction.TxManager.getManager() 
			).dumpTransactions();
	}
}
