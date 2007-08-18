package unit.neo.store;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.impl.nioneo.store.DynamicRecord;

public class TestDynamicStore extends TestCase
{

	public TestDynamicStore( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestDynamicStore.class );
		return suite;
	}
	
	
	public void setUp()
	{
	}
	
	public void tearDown()
	{
	}
	
	public void testCreateStore()
	{
		try
		{
			try
			{
				ByteStore.createStore( null, 1 );
				fail( "Null fileName should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
			try
			{
				ByteStore.createStore( "testDynamicStore.db", 0 );
				fail( "Illegal blocksize should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
			ByteStore.createStore( "testDynamicStore.db", 30 );
			try
			{
				ByteStore.createStore( "testDynamicStore.db", 15 );
				fail( "Creating existing store should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
		}
		catch ( IOException e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testDynamicStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testDynamicStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
			
	public void testStickyStore()
	{
		try
		{
			ByteStore.createStore( "testDynamicStore.db", 
				30 ).close();
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"testDynamicStore.db", "rw" ).getChannel();
			fileChannel.truncate( fileChannel.size() - 2 );
			try
			{
				new ByteStore( "testDynamicStore.db" );
				// assertTrue( !store.isStoreOk() );
			}
			catch ( IOException e )
			{ // good
			}
		}
		catch ( IOException e )
		{
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testDynamicStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testDynamicStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
	
	public void testClose()
	{
		try
		{
			ByteStore store = ByteStore.createStore( "testDynamicStore.db", 
				30 );
			int blockId = store.nextBlockId();
			Collection<DynamicRecord> records = store.allocateRecords( blockId,
				new byte[10] );
			for ( DynamicRecord record : records )
			{
				store.updateRecord( record );
			}
			store.close();
			try
			{
				store.allocateRecords( blockId, new byte[10] );
				fail( "Closed store should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
			try
			{
				store.getBytes( 0 );
				fail( "Closed store should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
			try
			{
				store.getLightRecords( 0, null );
				fail( "Closed store should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
		}
		catch ( IOException e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testDynamicStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testDynamicStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
	
	public void testStoreGetCharsFromString()
	{
		try
		{
			final String STR = 
				"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
			ByteStore store = ByteStore.createStore( "testDynamicStore.db", 
				30 );
			int blockId = store.nextBlockId();
			char[] chars = new char[ STR.length() ];
			STR.getChars( 0, STR.length(), chars, 0 );
			Collection<DynamicRecord> records = store.allocateRecords( blockId,
				chars );
			for ( DynamicRecord record : records )
			{
				store.updateRecord( record );
			}
//			assertEquals( STR, new String( store.getChars( blockId ) ) );
			store.close();
		}
		catch ( IOException e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testDynamicStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testDynamicStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
	
	public void testRandomTest() throws IOException
	{
		Random random =	new Random( System.currentTimeMillis() );
			ByteStore store = ByteStore.createStore( "testDynamicStore.db", 
				30 );
		java.util.ArrayList<Integer> idsTaken = 
			new java.util.ArrayList<Integer>();
		java.util.Map<Integer,byte[]> byteData = 
			new java.util.HashMap<Integer,byte[]>();
		float deleteIndex = 0.2f;
		float closeIndex = 0.1f;
		int currentCount = 0;
		int maxCount = 128;
		java.util.HashSet<Integer> set = new java.util.HashSet<Integer>();
		try
		{
			while ( currentCount < maxCount )
			{
				float rIndex = random.nextFloat();
				if ( rIndex < deleteIndex && currentCount > 0 )
				{
					int blockId = idsTaken.remove( random.nextInt( 
						currentCount ) ).intValue();
					store.getLightRecords( blockId, null );
					validateData( store.getBytes( blockId ), byteData.remove( 
						new Integer( blockId ) ) );
					Collection<DynamicRecord> records = store.getLightRecords( 
						blockId, null );
					for ( DynamicRecord record : records )
					{
						record.setInUse( false );
						store.updateRecord( record );
						set.remove( record.getId() );
					}
					currentCount--;
				}
				else
				{
					byte bytes[] = createRandomBytes( random );
					int blockId = store.nextBlockId();
					Collection<DynamicRecord> records = 
						store.allocateRecords( blockId, bytes );
					for ( DynamicRecord record : records )
					{
						assert !set.contains( record.getId() );
						store.updateRecord( record );
						set.add( record.getId() );
					}
					idsTaken.add( new Integer( blockId ) );
					byteData.put( new Integer( blockId ), bytes );
					currentCount++;
				}
				if ( rIndex > ( 1.0f - closeIndex ) || rIndex < closeIndex  )
				{
					store.close();
					store = new ByteStore( "testDynamicStore.db" );
				}
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testDynamicStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testDynamicStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
	
	private static class ByteStore extends AbstractDynamicStore
	{
		// store version, each store ends with this string (byte encoded)
		private static final String VERSION = "DynamicTestVersion v0.1";
		
		public ByteStore( String fileName ) 
			throws IOException
		{
			super( fileName );
		}
		
		public String getTypeAndVersionDescriptor()
		{
			return VERSION;
		}
		
		public static ByteStore createStore( String fileName, 
			int blockSize ) throws IOException
		{
			createEmptyStore( fileName, blockSize, VERSION );
			return new ByteStore( fileName ); 
		}
		
		public byte[] getBytes( int blockId ) throws IOException
		{
			return get( blockId );
		}
		
//		public char[] getChars( int blockId ) throws IOException
//		{
//			return getAsChar( blockId );
//		}
		
		public void flush()
		{
		}
	}

	private byte[] createRandomBytes( Random r )
	{
		return new byte[ r.nextInt( 1024 ) ];
	}
	
	private void validateData( byte data1[], byte data2[] )
	{
		assertEquals( data1.length, data2.length );
		for ( int i = 0; i < data1.length; i++ )
		{
			assertEquals( data1[i], data2[i] );
		}
	}
}
