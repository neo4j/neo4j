/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.BranchCollisionDetector;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.SideSelectorPolicies;
import org.neo4j.kernel.StandardBranchCollisionDetector;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.traversal.Evaluators.includeIfContainsAll;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.Traversal.bidirectionalTraversal;
import static org.neo4j.kernel.Traversal.traversal;
import static org.neo4j.kernel.Uniqueness.NODE_PATH;
import static org.neo4j.kernel.Uniqueness.RELATIONSHIP_PATH;

public class TestBidirectionalTraversal extends TraversalTestBase
{
    RelationshipType to = withName( "TO" );
    private Transaction tx;

    @Before
    public void init()
    {
        tx = beginTx();
    }

    @After
    public void tearDown()
    {
        tx.close();
    }
    
    @Test( expected = IllegalArgumentException.class )
    public void bothSidesMustHaveSameUniqueness() throws Exception
    {
        createGraph( "A TO B" );
        
        count( bidirectionalTraversal()
            .startSide( traversal().uniqueness( Uniqueness.NODE_GLOBAL ) )
            .endSide( traversal().uniqueness( Uniqueness.RELATIONSHIP_GLOBAL ) )
            .traverse( getNodeWithName( "A" ), getNodeWithName( "B" ) ) );
    }
    
    @Test
    public void pathsForOneDirection() throws Exception
    {
        /*
         * (a)-->(b)==>(c)-->(d)
         *   ^               /
         *    \--(f)<--(e)<-/
         */
        createGraph( "a TO b", "b TO c", "c TO d", "d TO e", "e TO f", "f TO a" );
        
        PathExpander<Void> expander = Traversal.pathExpanderForTypes( to, OUTGOING );
        expectPaths( bidirectionalTraversal()
                .mirroredSides( traversal().uniqueness( NODE_PATH ).expand( expander ) )
                .traverse( getNodeWithName( "a" ), getNodeWithName( "f" ) ), "a,b,c,d,e,f" );

        expectPaths( bidirectionalTraversal()
                .mirroredSides( traversal().uniqueness( RELATIONSHIP_PATH ).expand( expander ) )
                .traverse( getNodeWithName( "a" ), getNodeWithName( "f" ) ), "a,b,c,d,e,f", "a,b,c,d,e,f" );
    }
    
    @Test
    public void collisionEvaluator() throws Exception
    {
        /*
         *           (d)-->(e)--
         *            ^     |   \
         *            |     v    v
         *           (a)-->(b)<--(f)
         *            |    ^
         *            v   /
         *           (c)-/
         */
        createGraph( "a TO b", "a TO c", "c TO b", "a TO d", "d TO e", "e TO b", "e TO f", "f TO b" );
        
        PathExpander<Void> expander = PathExpanders.forTypeAndDirection( to, OUTGOING );
        BidirectionalTraversalDescription traversal = bidirectionalTraversal()
                .mirroredSides( traversal().uniqueness( NODE_PATH ).expand( expander ) );
        expectPaths( traversal
                .collisionEvaluator( includeIfContainsAll( getNodeWithName( "e" ) ) )
                .traverse( getNodeWithName( "a" ), getNodeWithName( "b" ) ), "a,d,e,b", "a,d,e,f,b" );
        expectPaths( traversal
                .collisionEvaluator( includeIfContainsAll( getNodeWithName( "e" ), getNodeWithName( "f" ) ) )
                .traverse( getNodeWithName( "a" ), getNodeWithName( "b" ) ), "a,d,e,f,b" );
    }
    
    @Test
    public void multipleCollisionEvaluators() throws Exception
    {
        /*
         *           (g)
         *           ^ \
         *          /   v
         *  (a)-->(b)   (c)
         *   |        --^ ^
         *   v       /    |
         *  (d)-->(e)----(f)
         */
        createGraph( "a TO b", "b TO g", "g TO c", "a TO d", "d TO e", "e TO c", "e TO f", "f TO c" );
        
        expectPaths( bidirectionalTraversal().mirroredSides( traversal().uniqueness( NODE_PATH ) )
            .collisionEvaluator( Evaluators.atDepth( 3 ) )
            .collisionEvaluator( Evaluators.includeIfContainsAll( getNodeWithName( "e" ) ) )
            .traverse( getNodeWithName( "a" ), getNodeWithName( "c" ) ),
            "a,d,e,c" );
    }
    
    @Test
    public void multipleStartAndEndNodes() throws Exception
    {
        /*
         * (a)--\         -->(f)
         *       v       /
         * (b)-->(d)<--(e)-->(g)
         *       ^
         * (c)--/
         */
        createGraph( "a TO d", "b TO d", "c TO d", "e TO d", "e TO f", "e TO g" );

        PathExpander<Void> expander = PathExpanderBuilder.<Void>empty().add( to ).build();
        TraversalDescription side = traversal().uniqueness( NODE_PATH ).expand( expander );
        expectPaths( bidirectionalTraversal().mirroredSides( side ).traverse(
                    asList( getNodeWithName( "a" ), getNodeWithName( "b" ), getNodeWithName( "c" ) ),
                    asList( getNodeWithName( "f" ), getNodeWithName( "g" ) ) ),
                    "a,d,e,f", "a,d,e,g", "b,d,e,f", "b,d,e,g", "c,d,e,f", "c,d,e,g" );
    }
    
    @Test
    public void ensureCorrectPathEntitiesInShortPath() throws Exception
    {
        /*
         * (a)-->(b)
         */
        createGraph( "a TO b" );
        
        Node a = getNodeWithName( "a" );
        Node b = getNodeWithName( "b" );
        Relationship r = a.getSingleRelationship( to, OUTGOING );
        Path path = single( bidirectionalTraversal()
            .mirroredSides( traversal().relationships( to, OUTGOING ).uniqueness( NODE_PATH ) )
            .collisionEvaluator( Evaluators.atDepth( 1 ) )
            .sideSelector( SideSelectorPolicies.LEVEL, 1 )
            .traverse( a, b ) );
        assertContainsInOrder( path.nodes(), a, b );
        assertContainsInOrder( path.reverseNodes(), b, a );
        assertContainsInOrder( path.relationships(), r );
        assertContainsInOrder( path.reverseRelationships(), r );
        assertContainsInOrder( path, a, r, b );
        assertEquals( a, path.startNode() );
        assertEquals( b, path.endNode() );
        assertEquals( r, path.lastRelationship() );
    }
    
    @Test
    public void mirroredTraversalReversesInitialState() throws Exception
    {
        /*
         * (a)-->(b)-->(c)-->(d)
         */
        createGraph( "a TO b", "b TO c", "c TO d" );

        BranchCollisionPolicy collisionPolicy = new BranchCollisionPolicy()
        {
            @Override
            public BranchCollisionDetector create( Evaluator evaluator )
            {
                return new StandardBranchCollisionDetector( null, null )
                {
                    @Override
                    protected boolean includePath( Path path, TraversalBranch startPath, TraversalBranch endPath )
                    {
                        assertEquals( 0, startPath.state() );
                        assertEquals( 10, endPath.state() );
                        return true;
                    }
                };
            }

            @Override
            public BranchCollisionDetector create( Evaluator evaluator, Predicate<Path> pathPredicate )
            {
                return create( evaluator );
            }
        };

        count( bidirectionalTraversal()
                // Just make up a number bigger than the path length (in this case 10) so that we can assert it in
                // the collision policy later
                .mirroredSides( traversal( NODE_PATH ).expand( PathExpanders.<Integer>forType( to ),
                        new InitialBranchState.State<>( 0, 10 ) ) )
                .collisionPolicy( collisionPolicy )
                .traverse( getNodeWithName( "a" ), getNodeWithName( "d" ) ) );
    }
}
