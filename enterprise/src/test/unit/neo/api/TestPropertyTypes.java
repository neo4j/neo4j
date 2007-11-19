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
	
	public void testShortType()
	{
		try
		{
			ut.begin();
			short value = 453;
			Short sValue = new Short( value );
			String key = "testshort";
			node1.setProperty( key, sValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Short propertyValue = null; 
			propertyValue = ( Short ) node1.getProperty( key );
			assertEquals( sValue, propertyValue );

			sValue = new Short( (short) 5335 ); 
			node1.setProperty( key, sValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Short ) node1.getProperty( key );
			assertEquals( sValue, propertyValue );
			
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
	
	public void testCharType()
	{
		try
		{
			ut.begin();
			char c = 'c';
			Character cValue = new Character( c );
			String key = "testchar";
			node1.setProperty( key, cValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			Character propertyValue = null; 
			propertyValue = ( Character ) node1.getProperty( key );
			assertEquals( cValue, propertyValue );

			cValue = new Character( 'd' ); 
			node1.setProperty( key, cValue );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( Character ) node1.getProperty( key );
			assertEquals( cValue, propertyValue );
			
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
	
	public void testIntArray()
	{
		try
		{
			ut.begin();
			int[] array1 = new int[] { 1, 2, 3, 4, 5 };
			Integer[] array2 = new Integer[] { 6, 7, 8 };
			String key = "testintarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			int propertyValue[] = null; 
			propertyValue = ( int[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( int[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Integer( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testShortArray()
	{
		try
		{
			ut.begin();
			short[] array1 = new short[] { 1, 2, 3, 4, 5 };
			Short[] array2 = new Short[] { 6, 7, 8 };
			String key = "testintarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			short propertyValue[] = null; 
			propertyValue = ( short[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( short[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Short( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testStringArray()
	{
		try
		{
			ut.begin();
			String[] array1 = new String[] { "a", "b", "c", "d", "e" };
			String[] array2 = new String[] { "ff", "gg", "hh" };
			String key = "teststringarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			String propertyValue[] = null; 
			propertyValue = ( String[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( String[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], propertyValue[i] );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testBooleanArray()
	{
		try
		{
			ut.begin();
			boolean[] array1 = new boolean[] { true, false, true, false, true };
			Boolean[] array2 = new Boolean[] { false, true, false };
			String key = "testboolarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			boolean propertyValue[] = null; 
			propertyValue = ( boolean[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( boolean[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Boolean( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testDoubleArray()
	{
		try
		{
			ut.begin();
			double[] array1 = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };
			Double[] array2 = new Double[] { 6.0, 7.0, 8.0 };
			String key = "testdoublearray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			double propertyValue[] = null; 
			propertyValue = ( double[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( double[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Double( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}	

	public void testFloatArray()
	{
		try
		{
			ut.begin();
			float[] array1 = new float[] { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
			Float[] array2 = new Float[] { 6.0f, 7.0f, 8.0f };
			String key = "testfloatarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			float propertyValue[] = null; 
			propertyValue = ( float[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( float[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Float( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}	

	public void testLongArray()
	{
		try
		{
			ut.begin();
			long[] array1 = new long[] { 1, 2, 3, 4, 5 };
			Long[] array2 = new Long[] { 6l, 7l, 8l };
			String key = "testlongarray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			long[] propertyValue = null; 
			propertyValue = ( long[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( long[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Long( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testByteArray()
	{
		try
		{
			ut.begin();
			byte[] array1 = new byte[] { 1, 2, 3, 4, 5 };
			Byte[] array2 = new Byte[] { 6, 7, 8 };
			String key = "testbytearray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			byte[] propertyValue = null; 
			propertyValue = ( byte[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( byte[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Byte( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testCharArray()
	{
		try
		{
			ut.begin();
			char[] array1 = new char[] { '1', '2', '3', '4', '5' };
			Character[] array2 = new Character[] { '6', '7', '8' };
			String key = "testchararray";
			node1.setProperty( key, array1 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			char[] propertyValue = null; 
			propertyValue = ( char[] ) node1.getProperty( key );
			assertEquals( array1.length, propertyValue.length );
			for ( int i = 0; i < array1.length; i++ )
			{
				assertEquals( array1[i], propertyValue[i] );
			}

			node1.setProperty( key, array2 );
			ut.commit();

			nm.clearCache();
			ut.begin();
			propertyValue = ( char[] ) node1.getProperty( key );
			assertEquals( array2.length, propertyValue.length );
			for ( int i = 0; i < array2.length; i++ )
			{
				assertEquals( array2[i], new Character( propertyValue[i] ) );
			}
			
			node1.removeProperty( key );
			ut.commit();
			
			nm.clearCache();
			ut.begin();
			assertTrue( !node1.hasProperty( key ) );
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
}
