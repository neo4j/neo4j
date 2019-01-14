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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertFoundRelationships;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodeCount;

public abstract class ExplicitIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.index().forNodes( "foo" ).add( graphDb.createNode(), "bar", "this is it" );
            Relationship edge = graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "LALA" ) );
            graphDb.index().forRelationships( "rels" ).add( edge, "alpha", "betting on the wrong string" );

            tx.success();
        }
    }

    @Test
    public void shouldFindNodeByLookup() throws Exception
    {
        // given
        try ( NodeExplicitIndexCursor cursor = cursors.allocateNodeExplicitIndexCursor();
              PrimitiveLongSet nodes = Primitive.longSet() )
        {
            // when
            indexRead.nodeExplicitIndexLookup( cursor, "foo", "bar", "this is it" );

            // then
            assertNodeCount( cursor, 1, nodes );

            // when
            indexRead.nodeExplicitIndexLookup( cursor, "foo", "bar", "not that" );

            // then
            assertNodeCount( cursor, 0, nodes );
        }
    }

    @Test
    public void shouldFindNodeByQuery() throws Exception
    {
        // given
        try ( NodeExplicitIndexCursor cursor = cursors.allocateNodeExplicitIndexCursor();
              PrimitiveLongSet nodes = Primitive.longSet() )
        {
            // when
            indexRead.nodeExplicitIndexQuery( cursor, "foo", "bar:this*" );

            // then
            assertNodeCount( cursor, 1, nodes );

            // when
            nodes.clear();
            indexRead.nodeExplicitIndexQuery( cursor, "foo", "bar", "this*" );

            // then
            assertNodeCount( cursor, 1, nodes );

            // when
            indexRead.nodeExplicitIndexQuery( cursor, "foo", "bar:that*" );

            // then
            assertNodeCount( cursor, 0, nodes );

            // when
            indexRead.nodeExplicitIndexQuery( cursor, "foo", "bar", "that*" );

            // then
            assertNodeCount( cursor, 0, nodes );
        }
    }

    @Test
    public void shouldFindRelationshipByLookup() throws Exception
    {
        // given
        try ( RelationshipExplicitIndexCursor cursor = cursors.allocateRelationshipExplicitIndexCursor();
              PrimitiveLongSet edges = Primitive.longSet() )
        {
            // when
            indexRead.relationshipExplicitIndexLookup(
                    cursor,
                    "rels",
                    "alpha",
                    "betting on the wrong string" ,
                    -1,
                    -1 );

            // then
            assertFoundRelationships( cursor, 1, edges );

            // when
            indexRead.relationshipExplicitIndexLookup( cursor, "rels", "bar", "not that", -1, -1 );

            // then
            assertFoundRelationships( cursor, 0, edges );
        }
    }

    @Test
    public void shouldFindRelationshipByQuery() throws Exception
    {
        // given
        try ( RelationshipExplicitIndexCursor cursor = cursors.allocateRelationshipExplicitIndexCursor();
              PrimitiveLongSet relationships = Primitive.longSet() )
        {
            // when
            indexRead.relationshipExplicitIndexQuery( cursor, "rels", "alpha:betting*", -1, -1 );

            // then
            assertFoundRelationships( cursor, 1, relationships );

            // when
            relationships.clear();
            indexRead.relationshipExplicitIndexQuery( cursor, "rels", "alpha", "betting*", -1,-1 );

            // then
            assertFoundRelationships( cursor, 1, relationships );

            // when
            indexRead.relationshipExplicitIndexQuery( cursor, "rels", "alpha:that*", -1, -1 );

            // then
            assertFoundRelationships( cursor, 0, relationships );

            // when
            indexRead.relationshipExplicitIndexQuery( cursor, "rels", "alpha", "that*", -1, -1 );

            // then
            assertFoundRelationships( cursor, 0, relationships );
        }
    }
}
