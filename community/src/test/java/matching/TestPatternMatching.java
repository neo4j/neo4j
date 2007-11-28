package matching;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.matching.PatternMatch;
import org.neo4j.util.matching.PatternMatcher;
import org.neo4j.util.matching.PatternNode;
import junit.framework.TestCase;

public class TestPatternMatching extends TestCase
{
	private EmbeddedNeo neo;
	private Transaction tx;
	
	private static enum MyRelTypes implements RelationshipType
	{
		R1,
		R2;
	}
	
	private Node createInstance( String name )
	{
		Node node = neo.createNode();
		node.setProperty( "name", name );
		return node;
	}
	
	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		neo = new EmbeddedNeo( "var/neo" );
		tx = Transaction.begin();
	}

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
		tx.finish();
		neo.shutdown();
	}
	
	public TestPatternMatching( String name )
	{
		super( name );
	}
	
	public void testTeethStructure()
	{
		final RelationshipType R1 = MyRelTypes.R1;
		final RelationshipType R2 = MyRelTypes.R2;
		
		Node aT = createInstance( "aType" );
		Node a1 = createInstance( "a1" );
		Node bT = createInstance( "bType" );
		Node b1 = createInstance( "b1" );
		Node cT = createInstance( "cType" );
		Node c1 = createInstance( "c1" );
		Node c2 = createInstance( "c2" );
		Node dT = createInstance( "dType" );
		Node d1 = createInstance( "d1" );
		Node d2 = createInstance( "d2" );
		Node eT = createInstance( "eType" );
		Node e1 = createInstance( "e1" );

		aT.createRelationshipTo( a1, R1 );
		bT.createRelationshipTo( b1, R1 );
		cT.createRelationshipTo( c1, R1 );
		cT.createRelationshipTo( c2, R1 );
		dT.createRelationshipTo( d1, R1 );
		dT.createRelationshipTo( d2, R1 );
		eT.createRelationshipTo( e1, R1 );

		a1.createRelationshipTo( b1, R2 );
		b1.createRelationshipTo( c1, R2 );
		b1.createRelationshipTo( c2, R2 );
		c1.createRelationshipTo( d1, R2 );
		c2.createRelationshipTo( d2, R2 );
		d1.createRelationshipTo( e1, R2 );
		d2.createRelationshipTo( e1, R2 );
	
		PatternNode pA = new PatternNode();
		PatternNode pAI = new PatternNode();
		pA.createRelationshipTo( pAI, R1 );
		PatternNode pB = new PatternNode();
		PatternNode pBI = new PatternNode();
		pB.createRelationshipTo( pBI, R1 );
		PatternNode pC = new PatternNode();
		PatternNode pCI = new PatternNode();
		pC.createRelationshipTo( pCI, R1 );
		PatternNode pD = new PatternNode();
		PatternNode pDI = new PatternNode();
		pD.createRelationshipTo( pDI, R1 );
		PatternNode pE = new PatternNode();
		PatternNode pEI = new PatternNode();
		pE.createRelationshipTo( pEI, R1 );
		
		pAI.createRelationshipTo( pBI, R2 );
		pBI.createRelationshipTo( pCI, R2 );
		pCI.createRelationshipTo( pDI, R2 );
		pDI.createRelationshipTo( pEI, R2 );
		
		int count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pA, aT ) )
		{
			assertEquals( match.getNodeFor( pA ), aT );
			assertEquals( match.getNodeFor( pAI ), a1 );
			assertEquals( match.getNodeFor( pB ), bT );
			assertEquals( match.getNodeFor( pBI ), b1 );
			assertEquals( match.getNodeFor( pC ), cT );
			Node c = match.getNodeFor( pCI );
			if ( !c.equals( c1 ) && !c.equals( c2 ) )
			{
				fail( "either c1 or c2" );
			}
			assertEquals( match.getNodeFor( pD ), dT );
			Node d = match.getNodeFor( pDI );
			if ( !d.equals( d1 ) && !d.equals( d2 ) )
			{
				fail( "either d1 or d2" );
			}
			assertEquals( match.getNodeFor( pE ), eT );
			assertEquals( match.getNodeFor( pEI ), e1 );
			count++;
		}
		assertEquals( 2, count );
		
		count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pCI, c2 ) )
		{
			assertEquals( match.getNodeFor( pA ), aT );
			assertEquals( match.getNodeFor( pAI ), a1 );
			assertEquals( match.getNodeFor( pB ), bT );
			assertEquals( match.getNodeFor( pBI ), b1 );
			assertEquals( match.getNodeFor( pC ), cT );
			assertEquals( match.getNodeFor( pCI ), c2 );
			assertEquals( match.getNodeFor( pD ), dT );
			assertEquals( match.getNodeFor( pDI ), d2 );
			assertEquals( match.getNodeFor( pE ), eT );
			assertEquals( match.getNodeFor( pEI ), e1 );
			count++;
		}
		assertEquals( 1, count );		
	}
	
	public void testNonCyclicABC()
	{
		Node a = createInstance( "A" );
		Node b1 = createInstance( "B1" );
		Node b2 = createInstance( "B2" );
		Node b3 = createInstance( "B3" );
		Node c = createInstance( "C" );
		
		final RelationshipType R = MyRelTypes.R1;
		
		a.createRelationshipTo( b1, R );
		a.createRelationshipTo( b2, R );
		a.createRelationshipTo( b3, R );
		b1.createRelationshipTo( c, R );
		b2.createRelationshipTo( c, R );
		b3.createRelationshipTo( c, R );
		
		PatternNode pA = new PatternNode();
		PatternNode pB = new PatternNode();
		PatternNode pC = new PatternNode();
		
		pA.createRelationshipTo( pB, R );
		pB.createRelationshipTo( pC, R );
		
		int count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pA, a ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			Node b = match.getNodeFor( pB );
			if ( !b.equals( b1 ) && !b.equals( b2 ) && !b.equals( b3 ) )
			{
				fail( "either b1 or b2 or b3" );
			}
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 3, count );
		count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pB, b2 ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			assertEquals( match.getNodeFor( pB ), b2 );
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 1, count );
	}
	
	public void testCyclicABC()
	{
		Node a = createInstance( "A" );
		Node b1 = createInstance( "B1" );
		Node b2 = createInstance( "B2" );
		Node b3 = createInstance( "B3" );
		Node c = createInstance( "C" );
		
		final RelationshipType R = MyRelTypes.R1;
		
		a.createRelationshipTo( b1, R );
		a.createRelationshipTo( b2, R );
		a.createRelationshipTo( b3, R );
		b1.createRelationshipTo( c, R );
		b2.createRelationshipTo( c, R );
		b3.createRelationshipTo( c, R );
		c.createRelationshipTo( a, R );
		
		PatternNode pA = new PatternNode();
		PatternNode pB = new PatternNode();
		PatternNode pC = new PatternNode();
		
		pA.createRelationshipTo( pB, R );
		pB.createRelationshipTo( pC, R );
		pC.createRelationshipTo( pA, R );
		
		int count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pA, a ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			Node b = match.getNodeFor( pB );
			if ( !b.equals( b1 ) && !b.equals( b2 ) && !b.equals( b3 ) )
			{
				fail( "either b1 or b2 or b3" );
			}
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 3, count );
		count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pB, b2 ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			Node b = match.getNodeFor( pB );
			if ( !b.equals( b1 ) && !b.equals( b2 ) && !b.equals( b3 ) )
			{
				fail( "either b1 or b2 or b3" );
			}
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 3, count );
	}

	public void testPropertyABC()
	{
		Node a = createInstance( "A" );
		a.setProperty( "hasProperty", true );
		Node b1 = createInstance( "B1" );
		b1.setProperty( "equals", 1 );
		Node b2 = createInstance( "B2" );
		b2.setProperty( "equals", 1 );
		Node b3 = createInstance( "B3" );
		b3.setProperty( "equals", 2 );
		Node c = createInstance( "C" );
		
		final RelationshipType R = MyRelTypes.R1;
		
		a.createRelationshipTo( b1, R );
		a.createRelationshipTo( b2, R );
		a.createRelationshipTo( b3, R );
		b1.createRelationshipTo( c, R );
		b2.createRelationshipTo( c, R );
		b3.createRelationshipTo( c, R );
		
		PatternNode pA = new PatternNode();
		pA.addPropertyExistConstraint( "hasProperty" );
		PatternNode pB = new PatternNode();
		pB.addPropertyEqualConstraint( "equals", 1 );
		PatternNode pC = new PatternNode();
		
		pA.createRelationshipTo( pB, R );
		pB.createRelationshipTo( pC, R );
		
		int count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pA, a ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			Node b = match.getNodeFor( pB );
			if ( !b.equals( b1 ) && !b.equals( b2 ) )
			{
				fail( "either b1 or b2" );
			}
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 2, count );
		count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pB, b2 ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			assertEquals( match.getNodeFor( pB ), b2 );
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 1, count );
	}

	public void testArrayPropertyValues()
	{
		Node a = createInstance( "A" );
		a.setProperty( "hasProperty", true );
		Node b1 = createInstance( "B1" );
		b1.setProperty( "equals", new Integer[] { 19, 1 } );
		Node b2 = createInstance( "B2" );
		b2.setProperty( "equals", new Integer[] { 1, 10, 12 } );
		Node b3 = createInstance( "B3" );
		b3.setProperty( "equals", 2 );
		Node c = createInstance( "C" );
		
		final RelationshipType R = MyRelTypes.R1;
		
		a.createRelationshipTo( b1, R );
		a.createRelationshipTo( b2, R );
		a.createRelationshipTo( b3, R );
		b1.createRelationshipTo( c, R );
		b2.createRelationshipTo( c, R );
		b3.createRelationshipTo( c, R );
		
		PatternNode pA = new PatternNode();
		pA.addPropertyExistConstraint( "hasProperty" );
		PatternNode pB = new PatternNode();
		pB.addPropertyEqualConstraint( "equals", 1 );
		PatternNode pC = new PatternNode();
		
		pA.createRelationshipTo( pB, R );
		pB.createRelationshipTo( pC, R );
		
		int count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pA, a ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			Node b = match.getNodeFor( pB );
			if ( !b.equals( b1 ) && !b.equals( b2 ) )
			{
				fail( "either b1 or b2" );
			}
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 2, count );
		count = 0;
		for ( PatternMatch match : 
			PatternMatcher.getMatcher().match( pB, b2 ) )
		{
			assertEquals( match.getNodeFor( pA ), a );
			assertEquals( match.getNodeFor( pB ), b2 );
			assertEquals( match.getNodeFor( pC ), c );
			count++;
		}
		assertEquals( 1, count );
	}
}
