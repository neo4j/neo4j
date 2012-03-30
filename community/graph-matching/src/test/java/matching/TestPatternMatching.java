/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package matching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphmatching.CommonValueMatchers;
import org.neo4j.graphmatching.PatternMatch;
import org.neo4j.graphmatching.PatternMatcher;
import org.neo4j.graphmatching.PatternNode;
import org.neo4j.graphmatching.PatternRelationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TestData;

import static java.util.Arrays.*;
import static org.junit.Assert.*;

public class TestPatternMatching implements GraphHolder
{
    @Override
    public GraphDatabaseService graphdb()
    {
        return graphDb;
    }

    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );

    private static GraphDatabaseService graphDb;
    private Transaction tx;

    private static enum MyRelTypes implements RelationshipType
    {
        R1,
        R2,
        R3,
        hasRoleInGroup,
        hasGroup,
        hasRole
    }

    private Node createInstance( String name )
    {
        Node node = graphDb.createNode();
        node.setProperty( "name", name );
        return node;
    }

    @BeforeClass
    public static void setUpDb()
    {
        graphDb = new EmbeddedGraphDatabase( "target/var/db" );
    }

    @Before
    public void setUpTx()
    {
        tx = graphDb.beginTx();
    }

    @After
    public void tearDownTx()
    {
        tx.finish();
    }

    @AfterClass
    public static void tearDownDb()
    {
        graphDb.shutdown();
    }

    private Iterable<PatternMatch> doMatch( PatternNode pNode )
    {
        return PatternMatcher.getMatcher().match( pNode, new HashMap<String, PatternNode>() );
    }

    private Iterable<PatternMatch> doMatch( PatternNode pNode, Node node )
    {
        return PatternMatcher.getMatcher().match( pNode, node, new HashMap<String, PatternNode>() );
    }

    private Iterable<PatternMatch> doMatch( PatternNode pNode, Node node, PatternNode... optionalNodes )
    {
        return PatternMatcher.getMatcher().match( pNode, node, new HashMap<String, PatternNode>(), optionalNodes );
    }

    @Test
    public void testAllRelTypes()
    {
        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;

        Node a1 = createInstance( "a1" );
        Node b1 = createInstance( "b1" );

        Set<Relationship> relSet = new HashSet<Relationship>();
        relSet.add( a1.createRelationshipTo( b1, R1 ) );
        relSet.add( a1.createRelationshipTo( b1, R2 ) );

        PatternNode pA = new PatternNode();
        PatternNode pB = new PatternNode();
        PatternRelationship pRel = pA.createRelationshipTo( pB );
        int count = 0;
        for ( PatternMatch match : doMatch( pA, a1 ) )
        {
            assertEquals( match.getNodeFor( pA ), a1 );
            assertEquals( match.getNodeFor( pB ), b1 );
            assertTrue( relSet.remove( match.getRelationshipFor( pRel ) ) );
            count++;
        }
        assertEquals( 0, relSet.size() );
        assertEquals( 2, count );
    }

    @Test
    public void testAllRelTypesWithRelProperty()
    {
        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;

        Node a1 = createInstance( "a1" );
        Node b1 = createInstance( "b1" );

        Relationship rel = a1.createRelationshipTo( b1, R1 );
        rel = a1.createRelationshipTo( b1, R2 );
        rel.setProperty( "musthave", true );

        PatternNode pA = new PatternNode();
        PatternNode pB = new PatternNode();
        PatternRelationship pRel = pA.createRelationshipTo( pB );
        pRel.addPropertyConstraint( "musthave", CommonValueMatchers.has() );
        int count = 0;
        for ( PatternMatch match : doMatch( pA, a1 ) )
        {
            assertEquals( match.getNodeFor( pA ), a1 );
            assertEquals( match.getNodeFor( pB ), b1 );
            count++;
        }
        assertEquals( 1, count );
    }

    @Test
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
        for ( PatternMatch match : doMatch( pA, aT ) )
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
        for ( PatternMatch match : doMatch( pCI, c2 ) )
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

    @Test
    public void testNonCyclicABC()
    {
        Node a = createInstance( "A" );
        Node b1 = createInstance( "B1" );
        Node b2 = createInstance( "B2" );
        Node b3 = createInstance( "B3" );
        Node c = createInstance( "C" );

        final RelationshipType R = MyRelTypes.R1;

        Relationship rAB1 = a.createRelationshipTo( b1, R );
        Relationship rAB2 = a.createRelationshipTo( b2, R );
        Relationship rAB3 = a.createRelationshipTo( b3, R );
        Relationship rB1C = b1.createRelationshipTo( c, R );
        Relationship rB2C = b2.createRelationshipTo( c, R );
        Relationship rB3C = b3.createRelationshipTo( c, R );

        PatternNode pA = new PatternNode();
        PatternNode pB = new PatternNode();
        PatternNode pC = new PatternNode();

        PatternRelationship pAB = pA.createRelationshipTo( pB, R );
        PatternRelationship pBC = pB.createRelationshipTo( pC, R );

        int count = 0;
        for ( PatternMatch match : doMatch( pA, a ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            Node b = match.getNodeFor( pB );
            if ( !b.equals( b1 ) && !b.equals( b2 ) && !b.equals( b3 ) )
            {
                fail( "either b1 or b2 or b3" );
            }
            Relationship rB = match.getRelationshipFor( pAB );
            if ( !rAB1.equals( rB ) && !rAB2.equals( rB ) && !rAB3.equals( rB ) )
            {
                fail( "either rAB1, rAB2 or rAB3" );
            }
            assertEquals( match.getNodeFor( pC ), c );
            Relationship rC = match.getRelationshipFor( pBC );
            if ( !rB1C.equals( rC ) && !rB2C.equals( rC ) && !rB3C.equals( rC ) )
            {
                fail( "either rB1C, rB2C or rB3C" );
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;
        for ( PatternMatch match : doMatch( pB, b2 ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            assertEquals( match.getNodeFor( pB ), b2 );
            assertEquals( match.getNodeFor( pC ), c );
            count++;
        }
        assertEquals( 1, count );
    }

    @Test
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
        for ( PatternMatch match : doMatch( pA, a ) )
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
        for ( PatternMatch match : doMatch( pB, b2 ) )
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
        assertEquals( 1, count );
    }

    @Test
    public void testPropertyABC()
    {
        Node a = createInstance( "A" );
        a.setProperty( "hasProperty", true );
        Node b1 = createInstance( "B1" );
        b1.setProperty( "equals", 1 );
        b1.setProperty( "name", "Thomas Anderson" );
        Node b2 = createInstance( "B2" );
        b2.setProperty( "equals", 1 );
        b2.setProperty( "name", "Thomas Anderson" );
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
        pA.addPropertyConstraint( "hasProperty", CommonValueMatchers.has() );
        PatternNode pB = new PatternNode();
        pB.addPropertyConstraint( "equals", CommonValueMatchers.exact( 1 ) );
        pB.addPropertyConstraint( "name", CommonValueMatchers.regex( Pattern.compile( "^Thomas.*" ) ) );
        PatternNode pC = new PatternNode();

        pA.createRelationshipTo( pB, R );
        pB.createRelationshipTo( pC, R );

        int count = 0;
        for ( PatternMatch match : doMatch( pA, a ) )
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
        for ( PatternMatch match : doMatch( pB, b2 ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            assertEquals( match.getNodeFor( pB ), b2 );
            assertEquals( match.getNodeFor( pC ), c );
            count++;
        }
        assertEquals( 1, count );
    }

    @Test
    public void testOptional()
    {
        Node a = createInstance( "A" );
        Node b1 = createInstance( "B1" );
        Node b2 = createInstance( "B2" );
        Node c = createInstance( "C" );
        Node d1 = createInstance( "D1" );
        Node d2 = createInstance( "D2" );
        Node e1 = createInstance( "E1" );
        Node e2 = createInstance( "E2" );
        Node f1 = createInstance( "F1" );
        Node f2 = createInstance( "F2" );
        Node f3 = createInstance( "F3" );

        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;
        final RelationshipType R3 = MyRelTypes.R3;
        a.createRelationshipTo( b1, R1 );
        a.createRelationshipTo( b2, R1 );
        a.createRelationshipTo( c, R2 );
        a.createRelationshipTo( f1, R3 );
        a.createRelationshipTo( f2, R3 );
        a.createRelationshipTo( f3, R3 );
        c.createRelationshipTo( d1, R1 );
        c.createRelationshipTo( d2, R1 );
        d1.createRelationshipTo( e1, R2 );
        d1.createRelationshipTo( e2, R2 );

        // Required part of the graph
        PatternNode pA = new PatternNode( "pA" );
        PatternNode pC = new PatternNode( "pC" );
        pA.createRelationshipTo( pC, R2 );

        // First optional branch
        PatternNode oA1 = new PatternNode( "pA" );
        PatternNode oB1 = new PatternNode( "pB" );
        oA1.createOptionalRelationshipTo( oB1, R1 );

        // // Second optional branch
        PatternNode oA2 = new PatternNode( "pA" );
        PatternNode oF2 = new PatternNode( "pF" );
        oA2.createOptionalRelationshipTo( oF2, R3 );

        // Third optional branch
        PatternNode oC3 = new PatternNode( "pC" );
        PatternNode oD3 = new PatternNode( "pD" );
        PatternNode oE3 = new PatternNode( "pE" );
        oC3.createOptionalRelationshipTo( oD3, R1 );
        oD3.createOptionalRelationshipTo( oE3, R2 );

        // Test that all permutations are there and that multiple optional
        // branches work.
        int count = 0;
        for ( PatternMatch match : doMatch( pA, a, oA1, oA2, oC3 ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            Node bMatch = match.getNodeFor( oB1 );
            if ( !bMatch.equals( b1 ) && !bMatch.equals( b2 ) )
            {
                fail( "either b1 or b2" );
            }
            Node fMatch = match.getNodeFor( oF2 );
            if ( !fMatch.equals( f1 ) && !fMatch.equals( f2 ) && !fMatch.equals( f3 ) )
            {
                fail( "either f1, f2 or f3" );
            }
            assertEquals( match.getNodeFor( pC ), c );
            assertEquals( match.getNodeFor( oD3 ), d1 );
            Node eMatch = match.getNodeFor( oE3 );
            assertTrue( eMatch.equals( e1 ) || eMatch.equals( e2 ) );
            count++;
        }
        assertEquals( count, 12 );

        // Test that unmatched optional branches are ignored.
        PatternNode pI = new PatternNode( "pI" );
        PatternNode pJ = new PatternNode( "pJ" );
        PatternNode pK = new PatternNode( "pK" );
        PatternNode pL = new PatternNode( "pL" );

        pI.createOptionalRelationshipTo( pJ, R1 );
        pI.createRelationshipTo( pK, R2 );
        pK.createOptionalRelationshipTo( pL, R2 );

        count = 0;
        for ( PatternMatch match : doMatch( pI, a, pI, pK ) )
        {
            assertEquals( match.getNodeFor( pI ), a );
            Node jMatch = match.getNodeFor( pJ );
            if ( !jMatch.equals( b1 ) && !jMatch.equals( b2 ) )
            {
                fail( "either b1 or b2" );
            }
            assertEquals( match.getNodeFor( pK ), c );
            assertEquals( match.getNodeFor( pL ), null );
            count++;
        }
        assertEquals( count, 2 );
    }

    @Test
    public void testOptional2()
    {
        Node a = createInstance( "A" );
        Node b1 = createInstance( "B1" );
        Node b2 = createInstance( "B2" );
        Node b3 = createInstance( "B3" );
        Node c1 = createInstance( "C1" );
        Node c3 = createInstance( "C3" );

        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;
        a.createRelationshipTo( b1, R1 );
        a.createRelationshipTo( b2, R1 );
        a.createRelationshipTo( b3, R1 );
        b1.createRelationshipTo( c1, R2 );
        b3.createRelationshipTo( c3, R2 );

        // Required part of the graph
        PatternNode pA = new PatternNode( "pA" );
        PatternNode pB = new PatternNode( "pB" );
        pA.createRelationshipTo( pB, R1 );

        // Optional part of the graph
        PatternNode oB = new PatternNode( "pB" );
        PatternNode oC = new PatternNode( "oC" );
        oB.createOptionalRelationshipTo( oC, R2 );

        int count = 0;
        for ( PatternMatch match : doMatch( pA, a, oB ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            Node bMatch = match.getNodeFor( pB );
            Node optionalBMatch = match.getNodeFor( oB );
            Node optionalCMatch = match.getNodeFor( oC );
            if ( !bMatch.equals( b1 ) && !bMatch.equals( b2 ) && !bMatch.equals( b3 ) )
            {
                fail( "either b1, b2 or b3" );
            }
            if ( optionalBMatch != null )
            {
                assertEquals( bMatch, optionalBMatch );
                if ( optionalBMatch.equals( b1 ) )
                {
                    assertEquals( optionalCMatch, c1 );
                }
                else if ( optionalBMatch.equals( b3 ) )
                {
                    assertEquals( optionalCMatch, c3 );
                }
                else
                {
                    assertEquals( optionalCMatch, null );
                }
            }
            count++;
        }
        assertEquals( count, 3 );
    }

    @Test
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
        pA.addPropertyConstraint( "hasProperty", CommonValueMatchers.has() );
        PatternNode pB = new PatternNode();
        pB.addPropertyConstraint( "equals", CommonValueMatchers.exactAny( 1 ) );
        PatternNode pC = new PatternNode();

        pA.createRelationshipTo( pB, R );
        pB.createRelationshipTo( pC, R );

        int count = 0;
        for ( PatternMatch match : doMatch( pA, a ) )
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
        for ( PatternMatch match : doMatch( pB, b2 ) )
        {
            assertEquals( match.getNodeFor( pA ), a );
            assertEquals( match.getNodeFor( pB ), b2 );
            assertEquals( match.getNodeFor( pC ), c );
            count++;
        }
        assertEquals( 1, count );
    }

    @Test
    public void testDiamond()
    {
        // C
        // / \
        // B---D
        // \ /
        // A
        Node a = createInstance( "A" );
        Node b = createInstance( "B" );
        Node c = createInstance( "C" );
        Node d = createInstance( "D" );

        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;

        a.createRelationshipTo( b, R1 );
        a.createRelationshipTo( d, R1 );
        b.createRelationshipTo( d, R2 );
        c.createRelationshipTo( b, R1 );
        c.createRelationshipTo( d, R1 );

        PatternNode pA = new PatternNode();
        PatternNode pB = new PatternNode();
        PatternNode pC = new PatternNode();
        PatternNode pD = new PatternNode();

        pA.createRelationshipTo( pB, R1, Direction.BOTH );
        pB.createRelationshipTo( pC, R2, Direction.BOTH );
        pC.createRelationshipTo( pD, R1, Direction.BOTH );

        int count = 0;
        for ( PatternMatch match : doMatch( pA, a ) )
        {
            count++;
        }
        assertEquals( 4, count );
    }

    @Test
    @Graph( { 
        "User1 hasRoleInGroup U1G1R12", 
        "U1G1R12 hasGroup Group1", 
        "U1G1R12 hasRole Role1",
        "U1G1R12 hasRole Role2", 
        "User1 hasRoleInGroup U1G2R23", 
        "U1G2R23 hasGroup Group2",
        "U1G2R23 hasRole Role2", 
        "U1G2R23 hasRole Role3", 
        "User1 hasRoleInGroup U1G3R34",
        "U1G3R34 hasGroup Group3", 
        "U1G3R34 hasRole Role3", 
        "U1G3R34 hasRole Role4",
        "User2 hasRoleInGroup U2G1R25", 
        "U2G1R25 hasGroup Group1", 
        "U2G1R25 hasRole Role2",
        "U2G1R25 hasRole Role5", 
        "User2 hasRoleInGroup U2G2R34", 
        "U2G2R34 hasGroup Group2",
        "U2G2R34 hasRole Role3", 
        "U2G2R34 hasRole Role4", 
        "User2 hasRoleInGroup U2G3R56",
        "U2G3R56 hasGroup Group3", 
        "U2G3R56 hasRole Role5", 
        "U2G3R56 hasRole Role6" 
        } )
    public void testHyperedges()
    {
        Map<String, Node> nodeMap = data.get();
        Node user1 = nodeMap.get( "User1" );

        PatternNode u1 = new PatternNode( "U1" );
        PatternNode u2 = new PatternNode( "U2" );
        PatternNode hyperEdge1 = new PatternNode( "UGR1" );
        PatternNode hyperEdge2 = new PatternNode( "UGR2" );
        PatternNode group = new PatternNode( "G" );
        PatternNode role = new PatternNode( "R" );

        u1.createRelationshipTo( hyperEdge1, MyRelTypes.hasRoleInGroup, Direction.OUTGOING );
        u2.createRelationshipTo( hyperEdge2, MyRelTypes.hasRoleInGroup, Direction.OUTGOING );

        hyperEdge1.createRelationshipTo( group, MyRelTypes.hasGroup, Direction.OUTGOING );
        hyperEdge1.createRelationshipTo( role, MyRelTypes.hasRole, Direction.OUTGOING );

        hyperEdge2.createRelationshipTo( group, MyRelTypes.hasGroup, Direction.OUTGOING );
        hyperEdge2.createRelationshipTo( role, MyRelTypes.hasRole, Direction.OUTGOING );

        u1.setAssociation( nodeMap.get( "User1" ) );
        u2.setAssociation( nodeMap.get( "User2" ) );

        List<Node> expected = new ArrayList<Node>( asList( nodeMap.get( "Group1" ), nodeMap.get( "Group2" ) ) );

        for ( PatternMatch match : doMatch( u1, nodeMap.get( "User1" ) ) )
        {
            Node matchedNode = match.getNodeFor( group );
            boolean remove = expected.remove( matchedNode );
            assertTrue( "Unexpected node matched: " + matchedNode.getProperty( "name" ), remove );
        }
        assertTrue( "Not all nodes were found", expected.isEmpty() );
    }

    private void execAndWait( String... args ) throws Exception
    {
        Process process = Runtime.getRuntime().exec( args );
        new ProcessStreamHandler( process, true ).waitForResult();
    }

    @Test
    public void testDiamondWithAssociation()
    {
        // C
        // / \
        // B---D
        // \ /
        // A
        Node a = createInstance( "A" );
        Node b = createInstance( "B" );
        Node c = createInstance( "C" );
        Node d = createInstance( "D" );

        final RelationshipType R1 = MyRelTypes.R1;
        final RelationshipType R2 = MyRelTypes.R2;

        a.createRelationshipTo( b, R1 );
        Relationship relAD = a.createRelationshipTo( d, R1 );
        b.createRelationshipTo( d, R2 );
        c.createRelationshipTo( b, R1 );
        c.createRelationshipTo( d, R1 );

        PatternNode pA = new PatternNode();
        PatternNode pB = new PatternNode();
        PatternNode pC = new PatternNode();
        PatternNode pD = new PatternNode();

        pA.createRelationshipTo( pB, R1, Direction.BOTH );
        pB.createRelationshipTo( pC, R2, Direction.BOTH );
        PatternRelationship lastRel = pC.createRelationshipTo( pD, R1, Direction.BOTH );

        pA.setAssociation( a );
        pB.setAssociation( b );
        pC.setAssociation( d );
        pD.setAssociation( a );

        int count = 0;
        for ( PatternMatch match : doMatch( pA ) )
        {
            count++;
        }
        assertEquals( 1, count );

        pD.setAssociation( null );
        count = 0;
        for ( PatternMatch match : doMatch( pA ) )
        {
            count++;
        }
        assertEquals( 2, count );

        lastRel.setAssociation( relAD );
        count = 0;
        for ( PatternMatch match : doMatch( pA ) )
        {
            count++;
        }
        assertEquals( 1, count );
    }
}
