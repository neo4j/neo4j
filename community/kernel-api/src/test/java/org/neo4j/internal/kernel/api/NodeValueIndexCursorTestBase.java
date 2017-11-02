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
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.ExplicitIndexCursorTestBase.assertFoundNodes;

public abstract class NodeValueIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
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
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "one" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "two" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "two" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "three" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "three" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", "three" );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 4 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 5 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 6 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 12 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 18 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 24 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 30 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 36 );
            graphDb.createNode( label( "Node" ) ).setProperty( "prop", 42 );

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
            assertFoundNodes( node, 0, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "one" ) );

            // then
            assertFoundNodes( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "two" ) );

            // then
            assertFoundNodes( node, 2, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, "three" ) );

            // then
            assertFoundNodes( node, 3, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexQuery.exact( prop, 6 ) );

            // then
            assertFoundNodes( node, 1, uniqueIds );
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
            assertFoundNodes( node, 5, uniqueIds );
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
            assertFoundNodes( node, 4, uniqueIds );
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
            assertFoundNodes( node, 3, uniqueIds );
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
            assertFoundNodes( node, 4, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", true, "three", false ) );

            // then
            assertFoundNodes( node, 1, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", false, "three", true ) );

            // then
            assertFoundNodes( node, 3, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", false, "two", false ) );

            // then
            assertFoundNodes( node, 3, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, "one", true, "two", true ) );

            // then
            assertFoundNodes( node, 6, uniqueIds );
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
            assertFoundNodes( node, 3, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, true, 12, false ) );

            // then
            assertFoundNodes( node, 2, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, false, 12, true ) );

            // then
            assertFoundNodes( node, 2, uniqueIds );

            // when
            uniqueIds.clear();
            read.nodeIndexSeek( index, node, IndexQuery.range( prop, 5, false, 12, false ) );

            // then
            assertFoundNodes( node, 1, uniqueIds );
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
            assertFoundNodes( node, 15, uniqueIds );
        }
    }
}
