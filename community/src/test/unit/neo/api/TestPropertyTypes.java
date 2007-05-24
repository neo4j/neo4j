package unit.neo.api;

import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.transaction.TransactionFactory;

public class TestPropertyTypes extends TestCase
{
	private NodeManager nm = NodeManager.getManager();
	private UserTransaction ut = TransactionFactory.getUserTransaction();
	
	private Node node1 = null;
	
	
	public TestPropertyTypes( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestPropertyTypes.class );
		return suite;
	}
	
	public void setUp()
	{
		try
		{
			ut.begin();
			node1 = nm.createNode();
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "Failed setup, " + e );
		}
	}
	
	public void tearDown()
	{
		try
		{
			ut.begin();
			node1 = nm.getNodeById( (int) node1.getId() );
			node1.delete();
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			// fail( "Failed tearDown, " + e );
		}
	}

	public void testDoubleType()
	{
		try
		{
			ut.begin();
			Double dValue = new Double( 45.678d );
			String key = "testdouble";
			node1.setProperty( key, dValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Double propertyValue = null; 
			propertyValue = ( Double ) node1.getProperty( key );
			assertEquals( dValue, propertyValue );

			dValue = new Double( 56784.3243d ); 
			node1.setProperty( key, dValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Double ) node1.getProperty( key );
			assertEquals( dValue, propertyValue );
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}

	public void testFloatType()
	{
		try
		{
			ut.begin();
			Float fValue = new Float( 45.678f );
			String key = "testfloat";
			node1.setProperty( key, fValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Float propertyValue = null; 
			propertyValue = ( Float ) node1.getProperty( key );
			assertEquals( fValue, propertyValue );

			fValue = new Float( 5684.3243f ); 
			node1.setProperty( key, fValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Float ) node1.getProperty( key );
			assertEquals( fValue, propertyValue );
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
	
	public void testLongType()
	{
		try
		{
			ut.begin();
			long time = System.currentTimeMillis();
			Long lValue = new Long( time );
			String key = "testlong";
			node1.setProperty( key, lValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Long propertyValue = null; 
			propertyValue = ( Long ) node1.getProperty( key );
			assertEquals( lValue, propertyValue );

			lValue = new Long( System.currentTimeMillis() ); 
			node1.setProperty( key, lValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Long ) node1.getProperty( key );
			assertEquals( lValue, propertyValue );
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}

	public void testByteType()
	{
		try
		{
			ut.begin();
			byte b = (byte) 177;
			Byte bValue = new Byte( b );
			String key = "testbyte";
			node1.setProperty( key, bValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Byte propertyValue = null; 
			propertyValue = ( Byte ) node1.getProperty( key );
			assertEquals( bValue, propertyValue );

			bValue = new Byte( (byte) 200 ); 
			node1.setProperty( key, bValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Byte ) node1.getProperty( key );
			assertEquals( bValue, propertyValue );
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
}
