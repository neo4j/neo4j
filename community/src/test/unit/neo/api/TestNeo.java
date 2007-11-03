package unit.neo.api;

import java.util.Random;
import javax.transaction.Status;
import javax.transaction.UserTransaction;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.core.NodeManager;
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
		TestSuite suite = new TestSuite( TestNeo.class );
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
//		assertFalse( nm.isValidRelationshipType( 
//			RelTypes.ONE_MORE_RELATIONSHIP ) );
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
//		assertFalse( nm.isValidRelationshipType( 
//			newRelType ) );
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
	
	public void testIdUsageInfo()
	{
		NodeManager nm = NodeManager.getManager();
		int nodeCount = nm.getNumberOfIdsInUse( Node.class );
		int relCount = nm.getNumberOfIdsInUse( Relationship.class );
		if ( nodeCount > nm.getHighestPossibleIdInUse( Node.class ) )
		{
			fail( "Node count greater than highest id " + nodeCount );
		}
		if ( relCount > nm.getHighestPossibleIdInUse( Relationship.class ) )
		{
			fail( "Rel count greater than highest id " + relCount );
		}
		assertTrue( nodeCount <= nm.getHighestPossibleIdInUse( Node.class ) );
		assertTrue( relCount <= nm.getHighestPossibleIdInUse( 
			Relationship.class ) );
		Node n1 = nm.createNode();
		Node n2 = nm.createNode();
		Relationship r1 = n1.createRelationshipTo( n2, MyRelTypes.TEST );
		assertEquals( nodeCount + 2, nm.getNumberOfIdsInUse( Node.class ) );
		assertEquals( relCount + 1, nm.getNumberOfIdsInUse( 
			Relationship.class ) );
		r1.delete();
		n1.delete();
		n2.delete();
		UserTransaction ut = TransactionFactory.getUserTransaction();
		// must commit for ids to be reused
		try
		{
			ut.commit();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
		assertEquals( nodeCount, nm.getNumberOfIdsInUse( Node.class ) );
		assertEquals( relCount, nm.getNumberOfIdsInUse( Relationship.class ) );
	}
	
	public void testRandomPropertyName()
	{
		Node node1 = NodeManager.getManager().createNode();
		String key = "random_" + new Random( 
			System.currentTimeMillis() ).nextLong();
		node1.setProperty( key, "value" );
		assertEquals( "value", node1.getProperty( key ) );
		node1.delete();
	}
	
	public void testNodeChangePropertyArray() throws Exception
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		ut.commit();
		Transaction tx = Transaction.begin();
		Node node;
		try
		{
			node = NodeManager.getManager().createNode();
			tx.success();
		}
		finally
		{
			tx.finish();
		}
		tx = Transaction.begin();
		try
		{
			node.setProperty( "test", new String[] { "value1" } );
			tx.success();
		}
		finally
		{
			tx.finish();
		}
		tx = Transaction.begin();
		try
		{
			node.setProperty( "test", new String[] { "value1", "value2" } );
			// no success, we wanna test rollback on this operation
		}
		finally
		{
			tx.finish();
		}
		tx = Transaction.begin();
		try
		{
			String[] value = (String[]) node.getProperty( "test" );
			assertEquals( 1, value.length );
			assertEquals( "value1", value[0] );
			tx.success();
		}
		finally
		{
			tx.finish();
		}
		ut.begin();
	}
}
