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

import java.util.Iterator;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Uniqueness;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.traversal.Evaluators.includeWhereEndNodeIs;
import static org.neo4j.kernel.Traversal.traversal;
import static org.neo4j.kernel.Uniqueness.NODE_GLOBAL;
import static org.neo4j.kernel.Uniqueness.NODE_LEVEL;
import static org.neo4j.kernel.Uniqueness.RELATIONSHIP_GLOBAL;
import static org.neo4j.kernel.Uniqueness.RELATIONSHIP_LEVEL;

public class TestUniqueness extends TraversalTestBase
{
    @Test
    public void nodeLevelUniqueness() throws Exception
    {
        /*
         *         (b)
         *       /  |  \
         *    (e)==(a)--(c)
         *       \  |
         *         (d)
         */

        createGraph( "a TO b", "a TO c", "a TO d", "a TO e", "a TO e", "b TO e", "d TO e", "c TO b" );
        RelationshipType to = withName( "TO" );
        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( "a" );
            Node e = getNodeWithName( "e" );
            Path[] paths = splitPathsOnePerLevel( traversal().relationships( to, OUTGOING )
                    .uniqueness( NODE_LEVEL ).evaluator( includeWhereEndNodeIs( e ) ).traverse( a ) );
            NodePathRepresentation pathRepresentation = new NodePathRepresentation( NAME_PROPERTY_REPRESENTATION );

            assertEquals( "a,e", pathRepresentation.represent( paths[1] ) );
            String levelTwoPathRepresentation = pathRepresentation.represent( paths[2] );
            assertTrue( levelTwoPathRepresentation.equals( "a,b,e" ) || levelTwoPathRepresentation.equals( "a,d,e" ) );
            assertEquals( "a,c,b,e", pathRepresentation.represent( paths[3] ) );
            tx.success();
        }
    }

    @Test
    public void nodeGlobalUniqueness()
    {
        /*
         * (a)-TO->(b)-TO->(c)
         *   \----TO---->/
         */
        createGraph( "a TO b", "a TO c", "b TO c" );
        RelationshipType to = withName( "TO" );

        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( "a" );
            Node c = getNodeWithName( "c" );
            Iterator<Path> path = traversal().relationships( to, OUTGOING ).uniqueness( NODE_GLOBAL ).evaluator(
                    includeWhereEndNodeIs( c ) ).traverse( a ).iterator();
            Path thePath = path.next();
            assertFalse( path.hasNext() );
            NodePathRepresentation pathRepresentation = new NodePathRepresentation( NAME_PROPERTY_REPRESENTATION );

            assertEquals( "a,b,c", pathRepresentation.represent( thePath ) );
            tx.success();
        }
    }

    @Test
    public void relationshipLevelAndGlobalUniqueness() throws Exception
    {
        /*
         *    (a)=TO=>(b)=TO=>(c)-TO->(d)
         *       \====TO====>/
         */

        createGraph( "a TO b", "b TO c", "a TO b", "b TO c", "a TO c", "a TO c", "c TO d" );
        RelationshipType to = withName( "TO" );


        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( "a" );
            Node d = getNodeWithName( "d" );

            Iterator<Path> paths = traversal().relationships( to, OUTGOING ).uniqueness( Uniqueness.NONE ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            int count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( "wrong number of paths calculated, the test assumption is wrong", 6, count );

            // Now do the same traversal but with unique per level relationships
            paths = traversal().relationships( to, OUTGOING ).uniqueness( RELATIONSHIP_LEVEL ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( "wrong number of paths calculated with relationship level uniqueness", 2, count );
            /*
            *  And yet again, but this time with global uniqueness, it should present only one path, since
            *  c TO d is contained on all paths.
            */
            paths = traversal().relationships( to, OUTGOING ).uniqueness( RELATIONSHIP_GLOBAL ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( "wrong number of paths calculated with relationship global uniqueness", 1, count );
        }
    }

    private Path[] splitPathsOnePerLevel( Traverser traverser )
    {
        Path[] paths = new Path[10];
        for ( Path path : traverser )
        {
            int depth = path.length();
            if ( paths[depth] != null )
            {
                fail( "More than one path one depth " + depth );
            }
            paths[depth] = path;
        }
        return paths;
    }
}
