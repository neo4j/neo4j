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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodeCount;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodes;

public abstract class NodeValueIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long strOne, strTwo1, strTwo2, strThree1, strThree2, strThree3;
    private static long boolTrue, num5, num6, num12a, num12b;

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        IndexDefinition index;
        try ( Transaction tx = graphDb.beginTx() )
        {
            index = graphDb.schema().indexFor( label( "Node" ) ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexOnline( index, 5, SECONDS );
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            strOne = nodeWithProp( graphDb, "one" );
            strTwo1 = nodeWithProp( graphDb, "two" );
            strTwo2 = nodeWithProp( graphDb, "two" );
            strThree1 = nodeWithProp( graphDb, "three" );
            strThree2 = nodeWithProp( graphDb, "three" );
            strThree3 = nodeWithProp( graphDb, "three" );
            nodeWithProp( graphDb, false );
            boolTrue = nodeWithProp( graphDb, true );
            nodeWithProp( graphDb, 4 );
            num5 = nodeWithProp( graphDb, 5 );
            num6 = nodeWithProp( graphDb, 6 );
            num12a = nodeWithProp( graphDb, 12.0 );
            num12b = nodeWithProp( graphDb, 12.0 );
            nodeWithProp( graphDb, 18 );
            nodeWithProp( graphDb, 24 );
            nodeWithProp( graphDb, 30 );
            nodeWithProp( graphDb, 36 );
            nodeWithProp( graphDb, 42 );

            tx.success();
        }
    }

    @Test
    public void shouldPerformExactLookup() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "zero" ) );

            // then
            assertNodes( node, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "one" ) );

            // then
            assertNodes( node, uniqueIds, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "two" ) );

            // then
            assertNodes( node, uniqueIds, strTwo1, strTwo2 );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "three" ) );

            // then
            assertNodes( node, uniqueIds, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, 6 ) );

            // then
            assertNodes( node, uniqueIds, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, 12.0 ) );

            // then
            assertNodes( node, uniqueIds, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, true ) );

            // then
            assertNodes( node, uniqueIds, boolTrue );
        }
    }

    @Test
    public void shouldPerformStringPrefixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexQuery.stringPrefix( prop, "t" ) );

            // then
            assertNodes( node, uniqueIds, strTwo1, strTwo2, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    public void shouldPerformStringSuffixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexQuery.stringSuffix( prop, "e" ) );

            // then
            assertNodes( node, uniqueIds, strOne, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    public void shouldPerformStringContainmentSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexQuery.stringContains( prop, "o" ) );

            // then
            assertNodes( node, uniqueIds, strOne, strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformStringRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertNodes( node, uniqueIds, strOne, strThree1, strThree2, strThree3 );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", true, "three", false ) );

            // then
            assertNodes( node, uniqueIds, strOne );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", false, "three", true ) );

            // then
            assertNodes( node, uniqueIds, strThree1, strThree2, strThree3 );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", false, "two", false ) );

            // then
            assertNodes( node, uniqueIds, strThree1, strThree2, strThree3 );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", true, "two", true ) );

            // then
            assertNodes( node, uniqueIds, strOne, strThree1, strThree2, strThree3, strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformNumericRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, true, 12, true ) );

            // then
            assertNodes( node, uniqueIds, num5, num6, num12a, num12b );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, true, 12, false ) );

            // then
            assertNodes( node, uniqueIds, num5, num6 );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, false, 12, true ) );

            // then
            assertNodes( node, uniqueIds, num6, num12a, num12b );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, false, 12, false ) );

            // then
            assertNodes( node, uniqueIds, num6 );
        }
    }

    @Test
    public void shouldPerformIndexScan() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexScan( index, node );

            // then
            assertNodeCount( node, 18, uniqueIds );
        }
    }

    @Test
    public void shouldGetNoIndex() throws Exception
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int badLabel = token.nodeLabel( "BAD_LABEL" );
        int badProp = token.propertyKey( "badProp" );

        assertEquals( "bad label", CapableIndexReference.NO_INDEX, schemaRead.index( badLabel, prop ) );
        assertEquals( "bad prop", CapableIndexReference.NO_INDEX, schemaRead.index( label, badProp ) );
        assertEquals( "just bad", CapableIndexReference.NO_INDEX, schemaRead.index( badLabel, badProp ) );
    }

    private long nodeWithProp( GraphDatabaseService graphDb, Object value )
    {
        Node node = graphDb.createNode( label( "Node" ) );
        node.setProperty( "prop", value );
        return node.getId();
    }
}
