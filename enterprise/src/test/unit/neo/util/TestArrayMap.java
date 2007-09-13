package unit.neo.util;

import org.neo4j.impl.util.ArrayMap;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestArrayMap extends TestCase
{
	public TestArrayMap( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestArrayMap.class );
		return suite;
	}
	
	public void setUp()
	{
	}
	
	public void tearDown()
	{
	}

	public void testArrayMap()
	{
		ArrayMap<String,Integer> map = new ArrayMap<String,Integer>();
		
		assertTrue( map.get( "key1" ) == null );
		map.put( "key1", 0 );
		assertEquals( new Integer(0), map.get( "key1" ) );
		assertEquals( new Integer(0), map.get( "key1" ) );
		map.put( "key1", 1 );
		assertEquals( new Integer(1), map.get( "key1" ) );
		map.put( "key2", 0 );
		assertEquals( new Integer(0), map.get( "key2" ) );
		map.put( "key2", 2 );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(2), map.remove( "key2" ) );
		assertTrue( map.get( "key2" ) == null );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(1), map.remove( "key1" ) );
		assertTrue( map.get( "key1" ) == null );

		map.put( "key1", 1 );
		map.put( "key2", 2 );
		map.put( "key3", 3 );
		map.put( "key4", 4 );
		map.put( "key5", 5 );
		assertEquals( new Integer(5), map.get( "key5" ) );
		assertEquals( new Integer(4), map.get( "key4" ) );
		assertEquals( new Integer(3), map.get( "key3" ) );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(5), map.remove( "key5" ) );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(4), map.get( "key4" ) );
		assertEquals( new Integer(3), map.get( "key3" ) );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(3), map.remove( "key3" ) );
		assertEquals( new Integer(1), map.remove( "key1" ) );
		assertEquals( new Integer(2), map.remove( "key2" ) );
		
		for ( int i = 0; i < 100; i++ )
		{
			map.put( "key" + i, i );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertEquals( new Integer(i), map.get( "key" + i) );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertEquals( new Integer(i), map.remove( "key" + i) );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertTrue( map.get( "key" + i ) == null );
		}
	}	
}
