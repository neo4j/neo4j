package unit.neo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.impl.nioneo.xa.XidImpl;

public class TestXa extends TestCase
{

	public TestXa( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestXa.class );
		return suite;
	}

	private NeoStoreXaDataSource ds;
	private NeoStoreXaConnection xaCon;
	private Logger log;
	private Level level;
	
	public void setUp()
	{
		log = Logger.getLogger( 
			"org.neo4j.impl.transaction.xaframework.XaLogicalLog/" + 
			"nioneo_logical.log" );
		level = log.getLevel();
		log.setLevel( Level.OFF );
		log = Logger.getLogger( 
			"org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource" );
		log.setLevel( Level.OFF );
		try
		{
			NeoStore.createStore( "neo" );
			ds = new NeoStoreXaDataSource( "neo", 
				"nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
	
	public void tearDown()
	{
		log.setLevel( level );
		log = Logger.getLogger( 
			"org.neo4j.impl.transaction.xaframework.XaLogicalLog/" + 
			"nioneo_logical.log" );
		log.setLevel( level );
		log = Logger.getLogger( 
			"org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource" );
		log.setLevel( level );
		File file = new File( "neo" );
		file.delete();
		file = new File( "neo.id" );
		file.delete();
		file = new File( "neo.nodestore.db" );
		file.delete();
		file = new File( "neo.nodestore.db.id" );
		file.delete();
		file = new File( "neo.propertystore.db" );
		file.delete();
		file = new File( "neo.propertystore.db.id" );
		file.delete();
		file = new File( "neo.propertystore.db.index" );
		file.delete();
		file = new File( "neo.propertystore.db.index.id" );
		file.delete();
		file = new File( "neo.propertystore.db.index.keys" );
		file.delete();
		file = new File( "neo.propertystore.db.index.keys.id" );
		file.delete();
		file = new File( "neo.propertystore.db.strings" );
		file.delete();
		file = new File( "neo.propertystore.db.strings.id" );
		file.delete();
		file = new File( "neo.propertystore.db.arrays" );
		file.delete();
		file = new File( "neo.propertystore.db.arrays.id" );
		file.delete();
		file = new File( "neo.relationshipstore.db" );
		file.delete();
		file = new File( "neo.relationshipstore.db.id" );
		file.delete();
		file = new File( "neo.relationshiptypestore.db" );
		file.delete();
		file = new File( "neo.relationshiptypestore.db.id" );
		file.delete();
		file = new File( "neo.relationshiptypestore.db.names" );
		file.delete();
		file = new File( "neo.relationshiptypestore.db.names.id" );
		file.delete();
		file = new File( "." );
		for ( File nioFile : file.listFiles() )
		{
			if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
			{
				nioFile.delete();
			}
		}
	}
	
	private void renameLogicalLog()
	{
		File file = new File( "." );
		for ( File nioFile : file.listFiles() )
		{
			if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
			{
				nioFile.renameTo( new File( "nioneo_logical.log" ) );
			}
		}
	}

	private void copyLogicalLog() throws IOException
	{
		FileChannel source = new RandomAccessFile( "nioneo_logical.log", 
			"r" ).getChannel();
		FileChannel dest = new RandomAccessFile( "nioneo_logical.log.bak", 
			"rw" ).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate( 1024 );
		int read = -1;
		do
		{
			read = source.read( buffer );
			buffer.flip();
			dest.write( buffer );
			buffer.clear();
		} while ( read == 1024 );
	}
	
	private PropertyIndex index( String key ) throws IOException
	{
		Iterator<PropertyIndex> itr = PropertyIndex.index( key ).iterator();
		if ( !itr.hasNext() )
		{
			int id = ds.nextId( PropertyIndex.class );
			PropertyIndex index = PropertyIndex.createDummyIndex( id, key );
			xaCon.getPropertyIndexConsumer().createPropertyIndex( id, key );
			// PropertyIndex.getIndexFor( id );
			return index;
		}
		return itr.next();
	}
	
	public void testLogicalLog()
	{
		try
		{
			Xid xid = new XidImpl( new byte[1], new byte[1] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
//			xaCon.getNodeConsumer().getProperties( node1 );
			int relType1 = ds.nextId( RelationshipType.class ); 
			xaCon.getRelationshipTypeConsumer().addRelationshipType(
				relType1, "relationshiptype1" );
			int rel1 = ds.nextId( Relationship.class );
			xaCon.getRelationshipConsumer().createRelationship( 
				rel1, node1, node2, relType1 );
			int r1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1, 
				index( "prop1" ), "string1" );
			xaCon.getNodeConsumer().changeProperty( node1, n1prop1, 
				"string2" );
			xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1, 
				"string2" );
			xaCon.getNodeConsumer().removeProperty( node1, n1prop1 );
			xaCon.getRelationshipConsumer().removeProperty( rel1, r1prop1 );
			xaCon.getRelationshipConsumer().deleteRelationship( rel1 );
			xaCon.getNodeConsumer().deleteNode( node1 );
			xaCon.getNodeConsumer().deleteNode( node2 );
			xaRes.end( xid, XAResource.TMSUCCESS );
			xaRes.commit( xid, true );
			xaCon.clearAllTransactions();
			ds.close();
			renameLogicalLog();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testLogicalLogPrepared()
	{
		try
		{
			Xid xid = new XidImpl( new byte[2], new byte[2] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
			int relType1 = ds.nextId( RelationshipType.class ); 
			xaCon.getRelationshipTypeConsumer().addRelationshipType(
				relType1, "relationshiptype1" );
			int rel1 = ds.nextId( Relationship.class );
			xaCon.getRelationshipConsumer().createRelationship( 
				rel1, node1, node2, relType1 );
			int r1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1, 
				index( "prop1" ), "string1" );
			xaCon.getNodeConsumer().changeProperty( node1, n1prop1, 
				"string2" );
			xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1, 
				"string2" );
			xaRes.end( xid, XAResource.TMSUCCESS );
			xaRes.prepare( xid );
			ds.close();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			// fileChannel.close();
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
			xaRes.commit( xid, true );
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testLogicalLogPrePrepared()
	{
		try
		{
			Xid xid = new XidImpl( new byte[3], new byte[3] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
			int relType1 = ds.nextId( RelationshipType.class ); 
			xaCon.getRelationshipTypeConsumer().addRelationshipType(
				relType1, "relationshiptype1" );
			int rel1 = ds.nextId( Relationship.class );
			xaCon.getRelationshipConsumer().createRelationship( 
				rel1, node1, node2, relType1 );
			int r1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1, 
				index( "prop1" ), "string1" );
			xaCon.getNodeConsumer().changeProperty( node1, n1prop1, 
				"string2" );
			xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1, 
				"string2" );
			xaRes.end( xid, XAResource.TMSUCCESS );
			xaCon.clearAllTransactions();
			ds.close();
			renameLogicalLog();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	} 
	
/*	public void testBrokenTimestamp()
	{
		try
		{
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"nioneo_logical.log", "rw" ).getChannel();
			fileChannel.truncate( 4 );
			fileChannel.close();
			LogicalLog.getLog().openLog( xaCon.getNeoStore() );
			File file = new File( "." );
			String files[] = file.list( new java.io.FilenameFilter() {
				public boolean accept( File file, String name )
				{
					return name.startsWith( 
						"nioneo_logical_unkown_timestamp_" );
				}  } );
			assertEquals( 1, files.length );
			new File( files[0] ).delete();
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}*/ 

	public void testBrokenCommand()
	{
		try
		{
			Xid xid = new XidImpl( new byte[4], new byte[4] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
			xaCon.clearAllTransactions();
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"nioneo_logical.log", "rw" ).getChannel();
			fileChannel.truncate( fileChannel.size() - 3 );
			fileChannel.force( false );
			fileChannel.close();
			ds.close();
			renameLogicalLog();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testBrokenPrepare()
	{
		try
		{
			Xid xid = new XidImpl( new byte[4], new byte[4] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
			xaRes.end( xid, XAResource.TMSUCCESS );
			xaRes.prepare( xid );
			xaCon.clearAllTransactions();
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"nioneo_logical.log", "rw" ).getChannel();
			fileChannel.truncate( fileChannel.size() - 3 );
			fileChannel.force( false );
			fileChannel.close();
			ds.close();
			renameLogicalLog();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}

	public void testBrokenDone()
	{
		try
		{
			Xid xid = new XidImpl( new byte[4], new byte[4] );
			XAResource xaRes = xaCon.getXaResource();
			xaRes.start( xid, XAResource.TMNOFLAGS );
			int node1 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node1 );
			int node2 = ds.nextId( Node.class );
			xaCon.getNodeConsumer().createNode( node2 );
			int n1prop1 = ds.nextId( PropertyStore.class );
			xaCon.getNodeConsumer().addProperty( node1, n1prop1, 
				index( "prop1" ), "string1" );
			xaRes.end( xid, XAResource.TMSUCCESS );
			xaRes.prepare( xid );
			xaRes.commit( xid, false );
			java.nio.channels.FileChannel fileChannel = 
				new java.io.RandomAccessFile( 
					"nioneo_logical.log", "rw" ).getChannel();
			fileChannel.truncate( fileChannel.size() - 3 );
			fileChannel.force( false );
			fileChannel.close();
			copyLogicalLog();
			ds.close();
			renameLogicalLog();
			ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log" );
			xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
			xaRes = xaCon.getXaResource();
			assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
			xaCon.clearAllTransactions();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
}
