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
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;

import static org.neo4j.graphdb.traversal.Evaluators.includeIfAcceptedByAny;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.Traversal.traversal;

public class TestMultipleFilters extends TraversalTestBase
{

    private Transaction tx;

    @Before
    public void setupGraph()
    {
        //
        //                     (a)--------
        //                     /          \
        //                    v            v
        //                  (b)-->(k)<----(c)-->(f)
        //                  / \
        //                 v   v
        //                (d)  (e)
        createGraph( "a TO b", "b TO d", "b TO e", "b TO k", "a TO c", "c TO f", "c TO k" );

        tx = beginTx();
    }

    @After
    public void tearDown()
    {
         tx.close();
    }
    
    private static class MustBeConnectedToNodeFilter implements Predicate<Path>, Evaluator
    {
        private final Node node;

        MustBeConnectedToNodeFilter( Node node )
        {
            this.node = node;
        }

        @Override
        public boolean test( Path item )
        {
            for ( Relationship rel : item.endNode().getRelationships( Direction.OUTGOING ) )
            {
                if ( rel.getEndNode().equals( node ) )
                {
                    return true;
                }
            }
            return false;
        }

        public Evaluation evaluate( Path path )
        {
            return test( path ) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }

    @Test
    public void testNarrowingFilters()
    {
        Evaluator mustBeConnectedToK = new MustBeConnectedToNodeFilter( getNodeWithName( "k" ) );
        Evaluator mustNotHaveMoreThanTwoOutRels = new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return Evaluation.ofIncludes( count( path.endNode().getRelationships( Direction.OUTGOING ) ) <= 2 );
            }
        };
        
        TraversalDescription description = traversal().evaluator( mustBeConnectedToK );
        expectNodes( description.traverse( node( "a" ) ), "b", "c" );
        expectNodes( description.evaluator( mustNotHaveMoreThanTwoOutRels ).traverse( node( "a" ) ), "c" );
    }
    
    @Test
    public void testBroadeningFilters()
    {
        MustBeConnectedToNodeFilter mustBeConnectedToC = new MustBeConnectedToNodeFilter( getNodeWithName( "c" ) );
        MustBeConnectedToNodeFilter mustBeConnectedToE = new MustBeConnectedToNodeFilter( getNodeWithName( "e" ) );
        
        // Nodes connected (OUTGOING) to c (which "a" is)
        expectNodes( traversal().evaluator( mustBeConnectedToC ).traverse( node( "a" ) ), "a" );
        // Nodes connected (OUTGOING) to c AND e (which none is)
        expectNodes( traversal().evaluator( mustBeConnectedToC ).evaluator( mustBeConnectedToE ).traverse( node( "a" ) ) );
        // Nodes connected (OUTGOING) to c OR e (which "a" and "b" is)
        expectNodes( traversal().evaluator( includeIfAcceptedByAny( mustBeConnectedToC, mustBeConnectedToE ) ).traverse( node( "a" ) ), "a", "b" );
    }
}
