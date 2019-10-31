/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.traversal.Evaluators.includeWhereEndNodeIs;
import static org.neo4j.graphdb.traversal.Uniqueness.NODE_GLOBAL;
import static org.neo4j.graphdb.traversal.Uniqueness.NODE_LEVEL;
import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_GLOBAL;
import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_LEVEL;

class TestUniqueness extends TraversalTestBase
{
    @Test
    void nodeLevelUniqueness()
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
            Node a = getNodeWithName( tx, "a" );
            Node e = getNodeWithName( tx, "e" );
            Path[] paths = splitPathsOnePerLevel( tx.traversalDescription().relationships( to, OUTGOING )
                    .uniqueness( NODE_LEVEL ).evaluator( includeWhereEndNodeIs( e ) ).traverse( a ) );
            NodePathRepresentation pathRepresentation = new NodePathRepresentation( NAME_PROPERTY_REPRESENTATION );

            assertEquals( "a,e", pathRepresentation.represent( paths[1] ) );
            String levelTwoPathRepresentation = pathRepresentation.represent( paths[2] );
            assertTrue( levelTwoPathRepresentation.equals( "a,b,e" ) || levelTwoPathRepresentation.equals( "a,d,e" ) );
            assertEquals( "a,c,b,e", pathRepresentation.represent( paths[3] ) );
            tx.commit();
        }
    }

    @Test
    void nodeGlobalUniqueness()
    {
        /*
         * (a)-TO->(b)-TO->(c)
         *   \----TO---->/
         */
        createGraph( "a TO c", "a TO b", "b TO c" );
        RelationshipType to = withName( "TO" );

        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( tx, "a" );
            Node c = getNodeWithName( tx, "c" );
            Iterator<Path> path = tx.traversalDescription()
                                    .relationships( to, OUTGOING )
                                    .uniqueness( NODE_GLOBAL ).evaluator( includeWhereEndNodeIs( c ) ).traverse( a ).iterator();
            Path thePath = path.next();
            assertFalse( path.hasNext() );
            NodePathRepresentation pathRepresentation = new NodePathRepresentation( NAME_PROPERTY_REPRESENTATION );

            assertEquals( "a,b,c", pathRepresentation.represent( thePath ) );
        }
    }

    @Test
    void relationshipLevelAndGlobalUniqueness()
    {
        /*
         *    (a)=TO=>(b)=TO=>(c)-TO->(d)
         *       \====TO====>/
         */

        createGraph( "a TO b", "b TO c", "a TO b", "b TO c", "a TO c", "a TO c", "c TO d" );
        RelationshipType to = withName( "TO" );

        try ( Transaction tx = beginTx() )
        {
            Node a = getNodeWithName( tx, "a" );
            Node d = getNodeWithName( tx, "d" );

            Iterator<Path> paths =
                    tx.traversalDescription().relationships( to, OUTGOING ).uniqueness( Uniqueness.NONE ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            int count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( 6, count, "wrong number of paths calculated, the test assumption is wrong" );

            // Now do the same traversal but with unique per level relationships
            paths = tx.traversalDescription().relationships( to, OUTGOING ).uniqueness( RELATIONSHIP_LEVEL ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( 2, count, "wrong number of paths calculated with relationship level uniqueness" );
            /*
            *  And yet again, but this time with global uniqueness, it should present only one path, since
            *  c TO d is contained on all paths.
            */
            paths = tx.traversalDescription().relationships( to, OUTGOING ).uniqueness( RELATIONSHIP_GLOBAL ).evaluator(
                    includeWhereEndNodeIs( d ) ).traverse( a ).iterator();
            count = 0;
            while ( paths.hasNext() )
            {
                count++;
                paths.next();
            }
            assertEquals( 1, count, "wrong number of paths calculated with relationship global uniqueness" );
        }
    }

    private static Path[] splitPathsOnePerLevel( Traverser traverser )
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
