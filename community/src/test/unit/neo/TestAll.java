package unit.neo;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;

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
		suite.addTest( unit.neo.util.TestAll.suite() );
		return suite;
	}
	
	private static NeoService neo;
	
	private static void startupNeo()
	{
		neo = new EmbeddedNeo( MyRelTypes.class, "var/neo" );
	}
	
	private static void shutdownNeo()
	{
		neo.shutdown();
	}
	
	public static void main( String args[] )
	{
		startupNeo();
		junit.textui.TestRunner.run( suite() );
		shutdownNeo();
		org.neo4j.impl.core.LockReleaser.getManager().dumpLocks();		
		org.neo4j.impl.transaction.LockManager.getManager().dumpRagStack();
		org.neo4j.impl.transaction.LockManager.getManager().dumpAllLocks();
		org.neo4j.impl.transaction.TxManager.getManager().dumpTransactions();
	}
}
