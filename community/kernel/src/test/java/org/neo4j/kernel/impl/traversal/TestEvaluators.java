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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;

import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_PRUNE;
import static org.neo4j.graphdb.traversal.Evaluators.includeWhereEndNodeIs;
import static org.neo4j.graphdb.traversal.Evaluators.lastRelationshipTypeIs;
import static org.neo4j.kernel.Traversal.description;
import static org.neo4j.kernel.Traversal.traversal;

public class TestEvaluators extends TraversalTestBase
{
    private enum Types implements RelationshipType
    {
        A,B,C
    }

    private Transaction tx;

    @Before
    public void createGraph()
    {
        /*
         * (a)--[A]->(b)--[B]-->(c)--[B]-->(d)--[C]-->(e)--[A]-->(j)
         *   \        |
         *   [B]     [C]-->(h)--[B]-->(i)--[C]-->(k)
         *     \
         *      v
         *      (f)--[C]-->(g)
         */
        
        createGraph( "a A b", "b B c", "c B d", "d C e", "e A j", "b C h", "h B i", "i C k",
                "a B f", "f C g" );

        tx = beginTx();
    }

    @After
    public void tearDown()
    {
        tx.close();
    }
    
    @Test
    public void lastRelationshipTypeEvaluator() throws Exception
    {
        Node a = getNodeWithName( "a" );
        expectPaths( traversal().evaluator( lastRelationshipTypeIs(
                INCLUDE_AND_PRUNE, EXCLUDE_AND_CONTINUE, Types.C ) ).traverse( a ),
                "a,b,c,d,e", "a,f,g", "a,b,h" );

        expectPaths( traversal().evaluator( lastRelationshipTypeIs(
                INCLUDE_AND_CONTINUE, EXCLUDE_AND_CONTINUE, Types.C ) ).traverse( a ),
                "a,b,c,d,e", "a,f,g", "a,b,h", "a,b,h,i,k" );
    }
    
    @Test
    public void endNodeIs()
    {
        Node a = getNodeWithName( "a" );
        Node c = getNodeWithName( "c" );
        Node h = getNodeWithName( "h" );
        Node g = getNodeWithName( "g" );
        
        expectPaths( description().evaluator( includeWhereEndNodeIs( c, h, g ) ).traverse( a ),
                "a,b,c", "a,b,h", "a,f,g" );
        expectPaths( description().evaluator( includeWhereEndNodeIs( g ) ).traverse( a ), "a,f,g" );
    }
    
    @Test
    public void depths() throws Exception
    {
        Node a = getNodeWithName( "a" );
        expectPaths( traversal().evaluator( Evaluators.atDepth( 1 ) ).traverse( a ), "a,b", "a,f" );
        expectPaths( traversal().evaluator( Evaluators.fromDepth( 2 ) ).traverse( a ), "a,f,g",
                "a,b,h", "a,b,h,i", "a,b,h,i,k", "a,b,c", "a,b,c,d", "a,b,c,d,e", "a,b,c,d,e,j" );
        expectPaths( traversal().evaluator( Evaluators.toDepth( 2 ) ).traverse( a ), "a", "a,b", "a,b,c",
                "a,b,h", "a,f", "a,f,g" );
    }
}
