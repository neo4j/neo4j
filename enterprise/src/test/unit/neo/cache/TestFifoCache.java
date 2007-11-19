/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package unit.neo.cache;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.cache.FifoCache;

public class TestFifoCache extends TestCase
{
	public TestFifoCache(String testName)
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestFifoCache.class );
		return suite;
	}
	
	public void testCreate()
	{
		try
		{
			new FifoCache<Object,Object>( "TestCache", 0 );
			fail( "Illegal maxSize should throw exception" );
		}
		catch ( IllegalArgumentException e )
		{ // good
		}
		FifoCache<Object,Object> cache = 
			new FifoCache<Object,Object>( "TestCache", 70 );
		try
		{
			cache.add( null, new Object() );
			fail( "Null key should throw exception" );
		}
		catch ( IllegalArgumentException e )
		{ // good
		}
		try
		{
			cache.add( new Object(), null );
			fail( "Null element should throw exception" );
		}
		catch ( IllegalArgumentException e )
		{ // good
		}
		try
		{
			cache.get( null );
			fail( "Null key should throw exception" );
		}
		catch ( IllegalArgumentException e )
		{ // good
		}
		try
		{
			cache.remove( null );
			fail( "Null key should throw exception" );
		}
		catch ( IllegalArgumentException e )
		{ // good
		}
		cache.add( new Object(), new Object() );
		cache.clear();
	}
	
	private static class FifoCacheTest<K,E> extends FifoCache<K,E>
	{
		private Object cleanedElement = null;
		
		FifoCacheTest( String name, int maxSize )
		{
			super( name, maxSize );
		}
		
		protected void elementCleaned( E element )
		{
			cleanedElement = element;
		}
		
		Object getLastCleanedElement()
		{
			return cleanedElement;
		}
	}
	
	
	public void testSimple()
	{
		FifoCacheTest<Object,Object> cache = 
			new FifoCacheTest<Object,Object>( "TestCache", 3 );
		String s1 = new String( "1" ); Integer key1 = new Integer( 1 );
		String s2 = new String( "2" ); Integer key2 = new Integer( 2 );
		String s3 = new String( "3" ); Integer key3 = new Integer( 3 );
		String s4 = new String( "4" ); Integer key4 = new Integer( 4 );
		String s5 = new String( "5" ); Integer key5 = new Integer( 5 );
		cache.add( key1, s1 );
		cache.add( key2, s2 );
		cache.add( key3, s3 );
		cache.get( key2 );
		assertEquals( null, cache.getLastCleanedElement() );
		cache.add( key4, s4 ); 
		assertEquals( s1, cache.getLastCleanedElement() );
		cache.add( key5, s5 );
		assertEquals( s2, cache.getLastCleanedElement() );
		int size = cache.size();
		assertEquals( 3, size );
		assertEquals( null, cache.get( key1 ) );
		assertEquals( null, cache.get( key2 ) );
		assertEquals( s3, cache.get( key3 ) );
		assertEquals( s4, cache.get( key4 ) );
		assertEquals( s5, cache.get( key5 ) );
		cache.clear();
		assertEquals( 0, cache.size() );
	}
}
