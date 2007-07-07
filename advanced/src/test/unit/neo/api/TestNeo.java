package unit.neo.api;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.IllegalValueException;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.TransactionFactory;

import unit.neo.MyRelTypes;

public class TestNeo extends TestCase
{
	public TestNeo(String testName)
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite();
		// suite.addTest( new TestNeo( "testNodePropertyPerformance" ) );
		// suite.addTest( new TestNeo( "testNodePropertyPerformance2" ) );
		// suite.addTest( new TestNeo( "testNodeIdxPerformance" ) );
		suite.addTest( new TestNeo( "testBasicNodeRelationships" ) );
		suite.addTest( new TestNeo( "testReferenceNode" ) );
		suite.addTest( new TestNeo( "testAddMoreRelationshipTypes" ) );
		suite.addTest( new TestNeo( "testAddMoreRelationshipTypes2" ) );
		// suite.addTest( new TestNeo( "testLotsAndLotsOfNodeRelationships" ) );
		return suite;
	}
	
	public void setUp()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() != Status.STATUS_NO_TRANSACTION )
			{
				fail ( "Status is not STATUS_NO_TRANSACTION but: " + 
					ut.getStatus() );
			}
			ut.begin();
		}
		catch ( Exception e )
		{
			fail( "Failed to start transaction, " + e );
		}
	}

	public void tearDown()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() == Status.STATUS_ACTIVE )
			{
				ut.commit();
			}
			else if ( ut.getStatus() == Status.STATUS_MARKED_ROLLBACK )
			{
				ut.rollback();
			}
			else if ( ut.getStatus() == Status.STATUS_NO_TRANSACTION )
			{
				// do nothing
			}
			else
			{
				System.out.println( "ARGH." );
				fail( "Unkown transaction status[" + ut.getStatus() + "]." );
			}
		}
		catch ( Exception e )
		{
			fail( "Failed to end transaciton, " + e );
		}
	}

	public void testReferenceNode()
	{
		// fix this test when we can set reference node again
/*		Node oldReferenceNode = null;
		// turn off logging since the code may print nasty stacktrace
		Logger log = Logger.getLogger( NodeManager.class.getName() );
		Level level = log.getLevel();
		log.setLevel( Level.OFF );
		try
		{
			// get old reference node if one is set
			oldReferenceNode = NodeManager.getManager().getReferenceNode();
		}
		catch ( NotFoundException e )
		{
			// ok no one set, oldReferenceNode is null then
		}
		log.setLevel( level );
		try
		{
			Node newReferenceNode = NodeManager.getManager().createNode();
			NeoJvmInstance.getConfig().getNeoModule().setReferenceNodeId( 
				(int) newReferenceNode.getId() );
			assertEquals( newReferenceNode, 
				NodeManager.getManager().getReferenceNode() );
			newReferenceNode.delete();
			if ( oldReferenceNode != null )
			{
				NeoJvmInstance.getConfig().getNeoModule().setReferenceNodeId( 
					(int) oldReferenceNode.getId() );
				assertEquals( oldReferenceNode, 
					NodeManager.getManager().getReferenceNode() );
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "" + e );
		}*/
	}
	
	public void testNodePropertyPerformance()
	{
		try
		{
			TransactionFactory.getTransactionManager().setTransactionTimeout( 
				120 );
		}
		catch ( javax.transaction.SystemException e )
		{
			fail( "Unable to set transaction timout." );
		}
		
		int amountOfNodes = 10000;
		String key = "key1";
		Object value = new Integer( 1 );
		
		// Create node space
		Node[] nodeSpace = new Node[amountOfNodes];
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			nodeSpace[i] = NodeManager.getManager().createNode();
		}
		
		// Populate node space
		Timer writeTime = new Timer();
		writeTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			if ( Math.random() < 0.80d )
			{
				try
				{
					nodeSpace[i].setProperty( key, value );
				}
				catch ( IllegalValueException e )
				{
					fail( "" + e );
				}
			}
			else
			{
				for ( int j = 1; j <= 13; j++ )
				{
					try
					{
						nodeSpace[i].setProperty( "key" + j, value );
					}
					catch ( IllegalValueException e )
					{
						fail( "" + e );
					}
				}
			}
		}
		writeTime.stop();
		
		// Read from node space
		Timer readTime = new Timer();
		readTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			try
			{
				Object val = nodeSpace[i].getProperty( key );
				assertEquals( value, val );
			}
			catch ( NotFoundException e )
			{
				fail( "" + e );
			}
		}
		readTime.stop();
		
		/*for ( int i = 0; i < nodeSpace.length; i++ )
		{
			try
			{
				nodeSpace[i].delete();
			}
			catch ( DeleteException e )
			{
				fail( "" + e );
			}
		}*/
		/*
		System.out.println( "\nProperty performance:" );
		System.out.println( "\tWrite " + amountOfNodes + " properties: " +
							writeTime.getTime() + "ms" );
		System.out.println( "\tRead " + amountOfNodes + " properties: " +
							readTime.getTime() + "ms" );
		 */
	}
	
	public void testNodePropertyPerformance2()
	{
		int amountOfNodes = 100000;
		String key = "key1";
		Object value = new Object();
		
		// Create node space
		Node[] nodeSpace = new Node[amountOfNodes];
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			nodeSpace[i] = NodeManager.getManager().createNode();
		}
		
		// Populate node space
		Timer writeTime = new Timer();
		writeTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			try
			{
				nodeSpace[i].setProperty( key, value );
			}
			catch ( IllegalValueException e )
			{
				fail( "" + e );
			}
		}
		writeTime.stop();
		
		// Read from node space
		Timer readTime = new Timer();
		readTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			try
			{
				Object val = nodeSpace[i].getProperty( key );
				assertEquals( value, val );
			}
			catch ( NotFoundException e )
			{
				fail( "" + e );
			}
		}
		readTime.stop();
		
		System.out.println( "\nProperty performance:" );
		System.out.println( "\tWrite " + amountOfNodes + " properties: " +
							writeTime.getTime() + "ms" );
		System.out.println( "\tRead " + amountOfNodes + " properties: " +
							readTime.getTime() + "ms" );
		for ( int i = 0; i < nodeSpace.length; i++ )
		{
			nodeSpace[i].delete();
		}
	}

	public void testNodePropertyIdxPerformance()
	{
		/* int amountOfNodes = 100000;
		String key = "key1";
		Object value = new Object();
		
		// Create node space
		NodeImplFastProp[] nodeSpace = new NodeImplFastProp[amountOfNodes];
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			nodeSpace[i] = (NodeImplFastProp)
								NodeManager.getManager().createNodePropIdx();
		}
		
		PropertyIndex idx = nodeSpace[0].getPropertyIndex( key );
		
		// Populate node space
		Timer writeTime = new Timer();
		writeTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			//PropertyIndex idx = nodeSpace[i].getPropertyIndex(key);
			nodeSpace[i].addProperty( idx, value );
		}
		writeTime.stop();
		
		// Read from node space
		Timer readTime = new Timer();
		readTime.start();
		for ( int i = 0; i < amountOfNodes; i++ )
		{
			//PropertyIndex idx = nodeSpace[i].getPropertyIndex(key);
			Object val = nodeSpace[i].getProperty( idx );
		}
		readTime.stop();
		
		System.out.println( "\nPropertyIdx performance:" );
		System.out.println( "\tWrite " + amountOfNodes + " properties: " +
							writeTime.getTime() + "ms" );
		System.out.println( "\tRead " + amountOfNodes + " properties: " +
							readTime.getTime() + "ms" );*/
	}
	
	// Simple timer class
	private class Timer
	{
		private long startTime = -1;
		private long endTime = -1;
		
		void start()
		{
			this.startTime = System.currentTimeMillis();
		}
		
		void stop()
		{
			this.endTime = System.currentTimeMillis();
		}
		
		long getTime()
		{
			return endTime - startTime;
		}
	}

	public void testBasicNodeRelationships()
	{
		Node firstNode = null;
		Node secondNode = null;
		Relationship rel = null;
		// Create nodes and a relationship between them
		firstNode = NodeManager.getManager().createNode();
		assertNotNull( "Failure creating first node", firstNode );
		secondNode = NodeManager.getManager().createNode();
		assertNotNull( "Failure creating second node", secondNode );
		rel = NodeManager.getManager().createRelationship(
			firstNode, secondNode, MyRelTypes.TEST );
		assertNotNull( "Relationship is null", rel );
		RelationshipType relType = rel.getType();
		assertNotNull( "Relationship's type is is null", relType );
	
		// Verify that the node reports that it has a relationship of
		// the type we created above
		assertTrue( firstNode.getRelationships( 
			relType ).iterator().hasNext() );
		assertTrue( secondNode.getRelationships( 
			relType ).iterator().hasNext() );
	
		Iterable<Relationship> allRels = null;

		// Verify that both nodes return the relationship we created above
		allRels = firstNode.getRelationships();
		assertTrue( this.objectExistsInIterable( rel, allRels ) );
		allRels = firstNode.getRelationships( relType );
		assertTrue( this.objectExistsInIterable( rel, allRels ) );
	
		allRels = secondNode.getRelationships();
		assertTrue( this.objectExistsInIterable( rel, allRels ) );
		allRels = secondNode.getRelationships( relType );
		assertTrue( this.objectExistsInIterable( rel, allRels ) );
	
		// Verify that the relationship reports that it is associated with
		// firstNode and secondNode
		Node[] relNodes = rel.getNodes();
		assertEquals( "A relationship should always be connected to exactly " +
					  "two nodes", relNodes.length, 2 );
		assertTrue( "Relationship says that it isn't connected to firstNode",
					this.objectExistsInArray( firstNode, relNodes ) );
		assertTrue( "Relationship says that it isn't connected to secondNode",
					this.objectExistsInArray( secondNode, relNodes ) );
		assertTrue( "The other node should be secondNode but it isn't", 
					rel.getOtherNode(firstNode).equals( secondNode ) );
		assertTrue( "The other node should be firstNode but it isn't",
					rel.getOtherNode(secondNode).equals( firstNode ) );
		rel.delete();
		secondNode.delete();
		firstNode.delete();
	}
//	
//	public void testLotsAndLotsOfNodeRelationships()
//	{
//		// Get n = 2*random(5, 10)
//		// Get r = random(n, 3*n)
//		// Create n nodes
//		// Loop from 1 to r
//		//	Create relationship between two nodes id=random(1, max n)
//
//		// Randomize
//		Random randGen = new Random();
//		int amountOfNodes = 2 * (10 + randGen.nextInt( 100 ));
//		int amountOfRels = amountOfNodes + randGen.nextInt( 2 * amountOfNodes );
//
//		// Create storage
//		Node[] allNodes = new Node[amountOfNodes];
//		Relationship[] allRels = new Relationship[amountOfRels];
//
//		// Create node space
//		for ( int i = 0; i < amountOfNodes; i++ )
//		{
//			allNodes[i] = NodeManager.getManager().createNode();
//		}
//
//		// Create relationships
//		for ( int i = 0; i < amountOfRels; i++ )
//		{
//			int firstNodeId = randGen.nextInt( amountOfNodes );
//			int secondNodeId = randGen.nextInt( amountOfNodes );
//
//			while ( secondNodeId == firstNodeId )
//			{
//				secondNodeId = randGen.nextInt( amountOfNodes );
//			}
//
//			allRels[i] = NodeManager.getManager().createRelationship(
//								allNodes[firstNodeId],
//								allNodes[secondNodeId],
//								RelationshipType.TEST,
//								false );
//		}
//
//		// Assert node space
//		for ( int i = 0; i < amountOfNodes; i++ )
//		{
//			this.assertNode(allNodes[i]);
//		}
//		
//		for ( int i = 0; i < allRels.length; i++ )
//		{
//			allRels[i].delete();
//		}
//		for ( int i = 0; i < allNodes.length; i++ )
//		{
//			allNodes[i].delete();
//		}
//	}
//	
//	private void assertNode( Node node )
//	{
//		assertNotNull( node );
//		Relationship[] relArray = node.getRelationships();
//		
//		for ( int i = 0; i < relArray.length; i++ )
//		{
//			// Scenario pic:
//			// [n1] <=r=> [n2]
//			
//			Node n1 = node;
//			Node n2 = null;
//			Relationship r = null;
//			
//			// Get r and n2
//			r = relArray[i];
//			RelationshipType rType = r.getType();
//			n2 = r.getOtherNode(n1);
//			assertNotNull(n2);
//			
//			// Verify that the relationship exists from n2's point of view 
//			// and that it leads back to n1
//			assertTrue( n2.hasRelationships(rType) );
//			Relationship[] n2Rels = n2.getRelationships( rType );
//			assertTrue( this.objectExistsInArray(r, n2Rels) );
//			assertSame( n1, r.getOtherNode(n2) );
//			
//			// Verify that r exists amongst n2's relationships
//			Relationship[] otherRels = n2.getRelationships();
//			assertTrue( this.objectExistsInArray( r, otherRels ) );
//		}
//	}
	
	
	private boolean objectExistsInIterable( Relationship rel, 
		Iterable<Relationship> allRels )
	{
		for ( Relationship iteratedRel : allRels )
		{
			if ( rel.equals( iteratedRel ) )
			{
				return true;
			}
		}
		return false;
	}

	private boolean objectExistsInArray( Object obj,
										 Object[] objArray )
	{
		for ( int i = 0; i < objArray.length; i++ )
		{
			if ( objArray[i].equals( obj ) )
			{
				return true;
			}
		}
		return false;
	}
	
	private static enum RelTypes implements RelationshipType
	{
		ONE_MORE_RELATIONSHIP;
	}
	
	public void testAddMoreRelationshipTypes()
	{
		NodeManager nm = NodeManager.getManager();
		assertFalse( nm.isValidRelationshipType( 
			RelTypes.ONE_MORE_RELATIONSHIP ) );
		nm.addEnumRelationshipTypes( RelTypes.class );
		assertTrue( nm.isValidRelationshipType( 
			RelTypes.ONE_MORE_RELATIONSHIP ) );
		Node node1 = nm.createNode();
		Node node2 = nm.createNode();
		Relationship rel = node1.createRelationshipTo( node2, 
			RelTypes.ONE_MORE_RELATIONSHIP );
		rel.delete();
		node2.delete();
		node1.delete();
	}

	public void testAddMoreRelationshipTypes2()
	{
		RelationshipType newRelType = new RelationshipType()
		{
			public String name()
			{
				return "ONE_MORE_RELTYPE_AGAIN";
			}
		};
		NodeManager nm = NodeManager.getManager();
		assertFalse( nm.isValidRelationshipType( 
			newRelType ) );
		nm.registerRelationshipType( newRelType.name(), true );
		assertTrue( nm.isValidRelationshipType( newRelType ) );
		Node node1 = nm.createNode();
		Node node2 = nm.createNode();
		Relationship rel = node1.createRelationshipTo( node2, 
			newRelType );
		rel.delete();
		node2.delete();
		node1.delete();
	}
}
