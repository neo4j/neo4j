package unit.neo.store;

import java.io.File;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.nioneo.store.AbstractStore;

public class TestStore extends TestCase
{

	public TestStore( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestStore.class );
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
				Store.createStore( null );
				fail( "Null fileName should throw exception" );
			}
			catch ( IOException e )
			{ // good
			}
			Store.createStore( "testStore.db" );
			try
			{
				Store.createStore( "testStore.db" );
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
			File file = new File( "testStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testStore.db.id" );
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
			Store.createStore( "testStore.db" ).close();
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"testStore.db", "rw" ).getChannel();
			fileChannel.truncate( fileChannel.size() - 2 );
			fileChannel.close();
			try
			{
				new Store( "testStore.db" );
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
			File file = new File( "testStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testStore.db.id" );
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
			Store store = Store.createStore( "testStore.db" ); 
			store.close();
		}
		catch ( IOException e )
		{
			fail( "" + e );
		}
		finally
		{
			File file = new File( "testStore.db" );
			if ( file.exists() )
			{
				file.delete();
			}
			file = new File( "testStore.db.id" );
			if ( file.exists() )
			{
				file.delete();
			}
		}
	}
	
	private static class Store extends AbstractStore
	{
		// store version, each store ends with this string (byte encoded)
		private static final String VERSION = "TestVersion v0.1";
		private static final int RECORD_SIZE = 1;
		
		public Store( String fileName ) 
			throws IOException
		{
			super( fileName );
		}
		
		protected void initStorage() throws IOException
		{
		}
		
		protected void closeImpl() throws IOException
		{
		}
		
		protected boolean fsck( boolean modify )
		{
			return false;
		}
		
		public int getRecordSize()
		{
			return RECORD_SIZE;
		}
	
		public String getTypeAndVersionDescriptor()
		{
			return VERSION;
		}
		
		public static Store createStore( String fileName ) throws IOException
		{
			createEmptyStore( fileName, VERSION );
			return new Store( fileName ); 
		}
		
		public void flush()
		{
		}
		
		protected void rebuildIdGenerator()
		{
		}
	}
}
