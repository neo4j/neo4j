/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodeCount;

public abstract class NodeLabelIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode( label( "One" ), label( "First" ) );
            graphDb.createNode( label( "Two" ), label( "First" ) );
            graphDb.createNode( label( "Three" ), label( "First" ) );
            graphDb.createNode( label( "Two" ) );
            graphDb.createNode( label( "Three" ) );
            graphDb.createNode( label( "Three" ) );

            tx.success();
        }
    }

    @Test
    public void shouldFindNodesByLabel() throws Exception
    {
        // given
        int one = token.nodeLabel( "One" );
        int two = token.nodeLabel( "Two" );
        int three = token.nodeLabel( "Three" );
        int first = token.nodeLabel( "First" );
        try ( NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeLabelScan( one, cursor );

            // then
            assertNodeCount( cursor, 1, uniqueIds );

            // when
            read.nodeLabelScan( two, cursor );

            // then
            assertNodeCount( cursor, 2, uniqueIds );

            // when
            read.nodeLabelScan( three, cursor );

            // then
            assertNodeCount( cursor, 3, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeLabelScan( first, cursor );

            // then
            assertNodeCount( cursor, 3, uniqueIds );
        }
    }

    @Test
    public void shouldFindNodesByDisjunction() throws Exception
    {
        // given
        int first = token.nodeLabel( "First" );
        int two = token.nodeLabel( "Two" );
        /// find nodes with the First Two labels...
        try ( NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeLabelUnionScan( cursor, first, two );

            // then
            assertNodeCount( cursor, 4, uniqueIds );
        }
    }

    @Test
    public void shouldFindNodesByConjunction() throws Exception
    {
        // given
        int first = token.nodeLabel( "First" );
        int two = token.nodeLabel( "Two" );
        // find the First node with label Two...
        try ( NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeLabelIntersectionScan( cursor, first, two );

            // then
            assertNodeCount( cursor, 1, uniqueIds );
        }
    }
}
