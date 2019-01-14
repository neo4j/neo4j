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

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CircularGraphTest extends TraversalTestBase
{
    @Before
    public void createTheGraph()
    {
        createGraph( "1 TO 2", "2 TO 3", "3 TO 1" );
    }

    @Test
    public void testCircularBug()
    {
        final long timestamp = 3;
        try ( Transaction tx = beginTx() )
        {
            getNodeWithName( "2" ).setProperty( "timestamp", 1L );
            getNodeWithName( "3" ).setProperty( "timestamp", 2L );
            tx.success();
        }

        try ( Transaction tx2 = beginTx() )
        {
            final RelationshipType type = RelationshipType.withName( "TO" );
            Iterator<Node> nodes = getGraphDb().traversalDescription()
                    .depthFirst()
                    .relationships( type, Direction.OUTGOING )
                    .evaluator( path ->
                    {
                        Relationship rel = path.lastRelationship();
                        boolean relIsOfType = rel != null && rel.isType( type );
                        boolean prune =
                                relIsOfType && (Long) path.endNode().getProperty( "timestamp" ) >= timestamp;
                        return Evaluation.of( relIsOfType, !prune );
                    } )
                    .traverse( node( "1" ) )
                    .nodes().iterator();

            assertEquals( "2", nodes.next().getProperty( "name" ) );
            assertEquals( "3", nodes.next().getProperty( "name" ) );
            assertFalse( nodes.hasNext() );
        }
    }
}
