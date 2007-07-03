package unit.neo.store;

import java.io.File;
import java.io.IOException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.impl.nioneo.xa.NodeEventConsumer;
import org.neo4j.impl.nioneo.xa.RelationshipEventConsumer;
import org.neo4j.impl.nioneo.xa.RelationshipTypeEventConsumer;
import org.neo4j.impl.nioneo.xa.XidImpl;

public class TestNeoStore extends TestCase
{

	public TestNeoStore( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestNeoStore.class );
		return suite;
	}
	
	private NodeEventConsumer nStore;
	private PropertyStore pStore;
	private RelationshipTypeEventConsumer relTypeStore;
	private RelationshipEventConsumer rStore;
	
	private NeoStoreXaDataSource ds;
	private NeoStoreXaConnection xaCon;
	
	public void setUp()
	{
		try
		{
			NeoStore.createStore( "neo" );
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
	
	private void initializeStores() throws IOException
	{
		try
		{
			ds = new NeoStoreXaDataSource( "neo", 
			"nioneo_logical.log" );
		}
		catch ( InstantiationException e )
		{
			throw new IOException( "" + e );
		}
		xaCon = ( NeoStoreXaConnection ) ds.getXaConnection();
		nStore = xaCon.getNodeConsumer(); 
		pStore = xaCon.getPropertyStore();
		relTypeStore = xaCon.getRelationshipTypeConsumer();
		rStore = xaCon.getRelationshipConsumer();
	}
	
	private Xid dummyXid; 
	private byte txCount = (byte) 0;
	XAResource xaResource;
	
	private void startTx() 
	{
		dummyXid = new XidImpl( new byte[txCount], new byte[txCount] );
		txCount++;
		xaResource = xaCon.getXaResource();
		try
		{
			xaResource.start( dummyXid, XAResource.TMNOFLAGS );
		}
		catch ( XAException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	private void commitTx()
	{
		try
		{
			xaResource.end( dummyXid, XAResource.TMSUCCESS );
			xaResource.commit( dummyXid, true );
		}
		catch ( XAException e )
		{
			throw new RuntimeException( e );
		}
		// xaCon.clearAllTransactions();
	}
	
	public void tearDown()
	{
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
		file = new File( "neo.propertystore.db.keys" );
		file.delete();
		file = new File( "neo.propertystore.db.keys.id" );
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
	
	public void testCreateNeoStore()
	{
		try
		{
			initializeStores();
			startTx();
			// setup test population
			int node1 = ds.nextId( Node.class );
			nStore.createNode( node1 );
			int node2 = ds.nextId( Node.class );
			nStore.createNode( node2 );
			int n1prop1 = pStore.nextId();
			int n1prop2 = pStore.nextId();
			int n1prop3 = pStore.nextId();
			nStore.addProperty( node1, n1prop1, "prop1", "string1" );
			nStore.addProperty( node1, n1prop2, "prop2", new Integer( 1 ) );
			nStore.addProperty( node1, n1prop3, "prop3", 
				new Boolean( true ) );
			int n2prop1 = pStore.nextId();
			int n2prop2 = pStore.nextId();
			int n2prop3 = pStore.nextId();
			nStore.addProperty( node2, n2prop1, "prop1", "string2" );
			nStore.addProperty( node2, n2prop2, "prop2", new Integer( 2 ) );
			nStore.addProperty( node2, n2prop3, "prop3", 
				new Boolean ( false ) );

			int relType1 = ds.nextId( RelationshipType.class ); 
			relTypeStore.addRelationshipType( relType1, "relationshiptype1" );
			int relType2 = ds.nextId( RelationshipType.class ); 
			relTypeStore.addRelationshipType( relType2, "relationshiptype2" );
			int rel1 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel1, node1, node2, relType1 );
			int rel2 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel2, node2, node1, relType2 );
			int r1prop1 = pStore.nextId();
			int r1prop2 = pStore.nextId();
			int r1prop3 = pStore.nextId();
			rStore.addProperty( rel1, r1prop1, "prop1", "string1" );
			rStore.addProperty( rel1, r1prop2, "prop2", new Integer( 1 ) );
			rStore.addProperty( rel1, r1prop3, "prop3", 
				new Boolean( true ) );
			int r2prop1 = pStore.nextId();
			int r2prop2 = pStore.nextId();
			int r2prop3 = pStore.nextId();
			rStore.addProperty( rel2, r2prop1, "prop1", "string2" );
			rStore.addProperty( rel2, r2prop2, "prop2", new Integer( 2 ) );
			rStore.addProperty( rel2, r2prop3, "prop3", 
				new Boolean ( false ) );
			commitTx();
			ds.close();
			
			initializeStores();
			startTx();
			// validate node
			validateNodeRel1( node1, n1prop1, n1prop2, n1prop3, rel1, rel2, 
				relType1, relType2 ); 
			validateNodeRel2( node2, n2prop1, n2prop2, n2prop3, rel1, rel2, 
				relType1, relType2 );
			// validate rels
			validateRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, 
				relType1 );
			validateRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, 
				relType2 );
			validateRelTypes( relType1, relType2 );
			// validate reltypes
			validateRelTypes( relType1, relType2 );
			commitTx();
			ds.close();

			initializeStores();
			startTx();
			// validate and delete rels
			deleteRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, 
				relType1 );
			deleteRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, 
				relType2 );
			// validate and delete nodes
			deleteNode1( node1, n1prop1, n1prop2, n1prop3 );
			deleteNode2( node2, n2prop1, n2prop2, n2prop3 );
			commitTx();
			ds.close();

			initializeStores();
			startTx();
			assertEquals( false, nStore.loadLightNode( node1 ) );
			assertEquals( false, nStore.loadLightNode( node2 ) );
			testGetRels( new int[] { rel1, rel2 } );
			// testGetProps( neoStore, new int[] { 
			//	n1prop1, n1prop2, n1prop3, n2prop1, n2prop2, n2prop3, 
			//	r1prop1, r1prop2, r1prop3, r2prop1, r2prop2, r2prop3
			//	} );
			int nodeIds[] = new int[10];
			for ( int i = 0; i < 3; i++ )
			{
				nodeIds[i] = ds.nextId( Node.class );
				nStore.createNode( nodeIds[i] );
				nStore.addProperty( nodeIds[i], pStore.nextId(), "nisse", 
					new Integer( 10 - i ) );
			}
			for ( int i = 0; i < 2; i++ )
			{
				rStore.createRelationship( ds.nextId( Relationship.class ), 
					nodeIds[i], nodeIds[i + 1],	relType1 );
			}
			for ( int i = 0; i < 3; i++ )
			{
				RelationshipData rels[] = nStore.getRelationships( 
						nodeIds[i] );
				for ( int j = 0; j < rels.length; j++ )
				{
					rStore.deleteRelationship( rels[j].getId() );
				}
				nStore.deleteNode( nodeIds[i] );
			}
			commitTx();
			ds.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	private void validateNodeRel1( int node, int prop1, int prop2, int prop3, 
		int rel1, int rel2, int relType1, int relType2 ) throws IOException
	{
		assertTrue( nStore.loadLightNode( node ) );
		PropertyData data[] = nStore.getProperties( node );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "string1", data[i].getValue() );
				nStore.changeProperty( node, prop1, "-string1" );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( 1 ), data[i].getValue() );
				nStore.changeProperty( node, prop2, new Integer( -1 ) );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( true ), data[i].getValue() );
				nStore.changeProperty( node, prop3, new Boolean( false ) );
			}
			else 
			{
				throw new IOException();
			}
		}
		RelationshipData rels[] = nStore.getRelationships( node );
		assertEquals( 2, rels.length );
		for ( int i = 0; i < 2; i++ )
		{
			if ( rels[i].getId() == rel1 )
			{
				RelationshipData rel = rels[i]; 
				assertEquals( node, rel.firstNode() );
				assertEquals( relType1, rel.relationshipType() );
			}
			else if ( rels[i].getId() == rel2 )
			{
				RelationshipData rel = rels[i];
				assertEquals( node, rel.secondNode() );
				assertEquals( relType2, rel.relationshipType() );
			}
			else 
			{
				throw new IOException();
			}
		}				
	}

	private void validateNodeRel2( int node, int prop1, int prop2, int prop3, 
		int rel1, int rel2, int relType1, int relType2 ) throws IOException
	{
		assertTrue( nStore.loadLightNode( node ) );
		PropertyData data[] = nStore.getProperties( node );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "string2", data[i].getValue() );
				nStore.changeProperty( node, prop1, "-string2" );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( 2 ), data[i].getValue() );
				nStore.changeProperty( node, prop2, new Integer( -2 ) );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( false ), data[i].getValue() );
				nStore.changeProperty( node, prop3, new Boolean( true ) );
			}
			else 
			{
				throw new IOException();
			}
		}
		RelationshipData rels[] = nStore.getRelationships( node );
		assertEquals( 2, rels.length );
		for ( int i = 0; i < 2; i++ )
		{
			if ( rels[i].getId() == rel1 )
			{
				// RelationshipData rel = rStore.getRelationship( rel1 );
				assertEquals( node, rels[i].secondNode() );
				assertEquals( relType1, rels[i].relationshipType() );
			}
			else if ( rels[i].getId() == rel2 )
			{
				// RelationshipData rel = rStore.getRelationship( rel2 );
				assertEquals( node, rels[i].firstNode() );
				assertEquals( relType2, rels[i].relationshipType() );
			}
			else 
			{
				throw new IOException();
			}
		}				
	}

	private void validateRel1( int rel, int prop1, int prop2, int prop3, 
		int firstNode, int secondNode, int relType ) throws IOException
	{
		PropertyData data[] = rStore.getProperties( rel );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "string1", data[i].getValue() );
				rStore.changeProperty( rel, prop1, "-string1" );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( 1 ), data[i].getValue() );
				rStore.changeProperty( rel, prop2, new Integer( -1 ) );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( true ), data[i].getValue() );
				rStore.changeProperty( rel, prop3, new Boolean( false ) );
			}
			else 
			{
				throw new IOException();
			}
		}
		RelationshipData relData = rStore.getRelationship( rel );
		assertEquals( firstNode, relData.firstNode() );
		assertEquals( secondNode, relData.secondNode() );
		assertEquals( relType, relData.relationshipType() );
	}

	private void validateRel2( int rel, int prop1, int prop2, int prop3, 
		int firstNode, int secondNode, int relType ) throws IOException
	{
		PropertyData data[] = rStore.getProperties( rel );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "string2", data[i].getValue() );
				rStore.changeProperty( rel, prop1, "-string2" );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( 2 ), data[i].getValue() );
				rStore.changeProperty( rel, prop2, new Integer( -2 ) );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( false ), data[i].getValue() );
				rStore.changeProperty( rel, prop3, new Boolean( true ) );
			}
			else 
			{
				throw new IOException();
			}
		}
		RelationshipData relData = rStore.getRelationship( rel );
		assertEquals( firstNode, relData.firstNode() );
		assertEquals( secondNode, relData.secondNode() );
		assertEquals( relType, relData.relationshipType() );
	}

	private void validateRelTypes( int relType1, int relType2 ) 
		throws IOException
	{
		RelationshipTypeData data = relTypeStore.getRelationshipType( 
			relType1 );
		assertEquals( relType1, data.getId() );
		assertEquals( "relationshiptype1", data.getName() );
		data = relTypeStore.getRelationshipType( relType2 );
		assertEquals( relType2, data.getId() );
		assertEquals( "relationshiptype2", data.getName() );
		RelationshipTypeData allData[] = relTypeStore.getRelationshipTypes();
		assertEquals( 2, allData.length );
		for ( int i = 0; i < 2; i++ )
		{
			if ( allData[i].getId() == relType1 )
			{
				assertEquals( relType1, allData[i].getId() );
				assertEquals( "relationshiptype1", allData[i].getName() );
			}
			else if ( allData[i].getId() == relType2 )
			{
				assertEquals( relType2, allData[i].getId() );
				assertEquals( "relationshiptype2", allData[i].getName() );
			}
			else 
			{
				throw new IOException();
			}
		}
	}

	private void deleteRel1( int rel, int prop1, int prop2, int prop3, 
		int firstNode, int secondNode, int relType ) throws IOException
	{
		PropertyData data[] = rStore.getProperties( rel );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "-string1", data[i].getValue() );
				// rStore.removeProperty( rel, prop1, r1prop1kb, r1prop1vb );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( -1 ), data[i].getValue() );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( false ), data[i].getValue() );
				rStore.removeProperty( rel, prop3 );
			}
			else 
			{
				throw new IOException();
			}
		}
		assertEquals( 2, rStore.getProperties( rel ).length );
		// rStore.removeProperty( rel, prop2 );
		RelationshipData relData = rStore.getRelationship( rel );
		assertEquals( firstNode, relData.firstNode() );
		assertEquals( secondNode, relData.secondNode() );
		assertEquals( relType, relData.relationshipType() );
		rStore.deleteRelationship( rel );
		assertEquals( 1, nStore.getRelationships( firstNode ).length );
		assertEquals( 1, nStore.getRelationships( secondNode ).length );
	}

	private void deleteRel2( int rel, int prop1, int prop2, int prop3, 
		int firstNode, int secondNode, int relType ) throws IOException
	{
		PropertyData data[] = rStore.getProperties( rel );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "-string2", data[i].getValue() );
				// rStore.removeProperty( rel, prop1, r2prop1kb, r2prop1vb );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( -2 ), data[i].getValue() );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( true ), data[i].getValue() );
				rStore.removeProperty( rel, prop3 );
			}
			else 
			{
				throw new IOException();
			}
		}
		assertEquals( 2, rStore.getProperties( rel ).length );
		RelationshipData relData = rStore.getRelationship( rel );
		assertEquals( firstNode, relData.firstNode() );
		assertEquals( secondNode, relData.secondNode() );
		assertEquals( relType, relData.relationshipType() );
		rStore.deleteRelationship( rel );
		assertEquals( 0, nStore.getRelationships( firstNode ).length );
		assertEquals( 0, nStore.getRelationships( secondNode ).length );
	}

	private void deleteNode1( int node, int prop1, int prop2, int prop3 ) 
		throws IOException
	{
		PropertyData data[] = nStore.getProperties( node );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "-string1", data[i].getValue() );
				// nStore.removeProperty( node, prop1, n1prop1kb, n1prop1vb );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( -1 ), data[i].getValue() );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( false ), data[i].getValue() );
				nStore.removeProperty( node, prop3 );
			}
			else 
			{
				throw new IOException();
			}
		}
		assertEquals( 2, nStore.getProperties( node ).length );
		// nStore.removeProperty( node, prop2 );
		assertEquals( 0, nStore.getRelationships( node ).length );
		nStore.deleteNode( node );
	}

	private void deleteNode2( int node, int prop1, int prop2, int prop3 ) 
		throws IOException
	{
		PropertyData data[] = nStore.getProperties( node );
		for ( int i = 0; i < data.length; i++ )
		{
			data[i] = new PropertyData( data[i].getId(), data[i].getKey(), 
				pStore.getPropertyValue( data[i].getId() ),
				data[i].nextPropertyId() );
		}
		assertEquals( 3, data.length );
		for ( int i = 0; i < 3; i++ )
		{
			if ( data[i].getId() == prop1 )
			{
				assertEquals( "prop1", data[i].getKey() );
				assertEquals( "-string2", data[i].getValue() );
				// nStore.removeProperty( node, prop1, n2prop1kb, n2prop1vb );
			}
			else if ( data[i].getId() == prop2 )
			{
				assertEquals( "prop2", data[i].getKey() );
				assertEquals( new Integer( -2 ), data[i].getValue() );
			}
			else if ( data[i].getId() == prop3 )
			{
				assertEquals( "prop3", data[i].getKey() );
				assertEquals( new Boolean( true ), data[i].getValue() );
				nStore.removeProperty( node, prop3 );
			}
			else 
			{
				throw new IOException();
			}
		}
		assertEquals( 2, nStore.getProperties( node ).length );
		// nStore.removeProperty( node, prop2 );
		assertEquals( 0, nStore.getRelationships( node ).length );
		nStore.deleteNode( node );
	}
	
	private void testGetRels( int relIds[] )
	{
		for ( int i = 0; i < relIds.length; i++ )
		{
			try
			{
				rStore.getRelationship( relIds[i] );
				fail( "Got deleted relationship[" + relIds[i] + "]" );
			}
			catch ( IOException e )
			{ // good
			}
		}
	}

	public void testRels1()
	{
		try
		{
			initializeStores();
			startTx();
			int relType1 = ds.nextId( RelationshipType.class ); 
			relTypeStore.addRelationshipType( relType1, "relationshiptype1" );
			int nodeIds[] = new int[3];
			for ( int i = 0; i < 3; i++ )
			{
				nodeIds[i] = ds.nextId( Node.class );
				nStore.createNode( nodeIds[i] );
				nStore.addProperty( nodeIds[i], pStore.nextId(), "nisse", 
					new Integer( 10 - i ) );
			}
			for ( int i = 0; i < 2; i++ )
			{
				rStore.createRelationship( ds.nextId( Relationship.class ), 
					nodeIds[i], nodeIds[i + 1],	relType1 );
			}
			commitTx();
			startTx();
			for ( int i = 0; i < 3; i++ )
			{
				RelationshipData rels[] = nStore.getRelationships( 
					nodeIds[i] );
				for ( int j = 0; j < rels.length; j++ )
				{
					rStore.deleteRelationship( rels[j].getId() );
				}
				nStore.deleteNode( nodeIds[i] );
			}
			commitTx();
			ds.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testRels2()
	{
		try
		{
			initializeStores();
			startTx();
			int relType1 = ds.nextId( RelationshipType.class ); 
			relTypeStore.addRelationshipType( relType1, "relationshiptype1" );
			int nodeIds[] = new int[3];
			for ( int i = 0; i < 3; i++ )
			{
				nodeIds[i] = ds.nextId( Node.class );
				nStore.createNode( nodeIds[i] );
				nStore.addProperty( nodeIds[i], pStore.nextId(), "nisse", 
					new Integer( 10 - i ) );
			}
			for ( int i = 0; i < 2; i++ )
			{
				rStore.createRelationship( ds.nextId( Relationship.class ), 
					nodeIds[i], nodeIds[i + 1],	relType1 );
			}
			rStore.createRelationship( ds.nextId( Relationship.class ),  
				nodeIds[0], nodeIds[2], relType1 );
			commitTx();
			startTx();
			for ( int i = 0; i < 3; i++ )
			{
				RelationshipData rels[] = 
					nStore.getRelationships( nodeIds[i] );
				for ( int j = 0; j < rels.length; j++ )
				{
					rStore.deleteRelationship( rels[j].getId() );
				}
				nStore.deleteNode( nodeIds[i] );
			}
			commitTx();
			ds.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testRels3()
	{
		// test linked list stuff during relationship delete
		try
		{
			initializeStores();
			startTx();
			int relType1 = ds.nextId( RelationshipType.class ); 
			relTypeStore.addRelationshipType( relType1, "relationshiptype1" );
			int nodeIds[] = new int[8];
			for ( int i = 0; i < nodeIds.length; i++ )
			{
				nodeIds[i] = ds.nextId( Node.class );
				nStore.createNode( nodeIds[i] );
			}
			for ( int i = 0; i < nodeIds.length/2; i++ )
			{
				rStore.createRelationship( ds.nextId( Relationship.class ), 
					nodeIds[i], nodeIds[i*2], relType1 );
			}
			int rel5= ds.nextId( Relationship.class );
			rStore.createRelationship( rel5, nodeIds[0], nodeIds[5], 
				relType1 );
			int rel2 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel2, nodeIds[1], nodeIds[2], 
				relType1 );
			int rel3 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel3, nodeIds[1], nodeIds[3], 
				relType1 );
			int rel6= ds.nextId( Relationship.class );
			rStore.createRelationship( rel6, nodeIds[1], nodeIds[6], 
				relType1 );
			int rel1 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel1, nodeIds[0], nodeIds[1], 
				relType1 );
			int rel4 = ds.nextId( Relationship.class );
			rStore.createRelationship( rel4, nodeIds[0], nodeIds[4], 
				relType1 );
			int rel7= ds.nextId( Relationship.class );
			rStore.createRelationship( rel7, nodeIds[0], nodeIds[7], 
				relType1 );
			commitTx();
			startTx();
			rStore.deleteRelationship( rel7 );
			rStore.deleteRelationship( rel4 );
			rStore.deleteRelationship( rel1 );
			rStore.deleteRelationship( rel6 );
			rStore.deleteRelationship( rel3 );
			rStore.deleteRelationship( rel2 );
			rStore.deleteRelationship( rel5 );			
//			nStore.deleteNode( nodeIds[2] );
//			nStore.deleteNode( nodeIds[3] );
//			nStore.deleteNode( nodeIds[1] );
//			nStore.deleteNode( nodeIds[4] );
//			nStore.deleteNode( nodeIds[0] );			
			commitTx();
			ds.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testProps1()
	{
		try
		{
			initializeStores();
			startTx();
			int nodeId = ds.nextId( Node.class );
			nStore.createNode( nodeId );
			int propertyId = pStore.nextId();
			nStore.addProperty( nodeId, propertyId, "nisse", 
				new Integer( 10 ) );
			commitTx();
			ds.close();
			initializeStores();
			startTx();
			nStore.changeProperty( nodeId, propertyId, new Integer( 5 ) );
			nStore.removeProperty( nodeId, propertyId );
			nStore.deleteNode( nodeId );
			commitTx();
			ds.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
}
