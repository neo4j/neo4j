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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEvent;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnAllNodesScan;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnIndexSeek;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnLabelScan;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnNode;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnProperty;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnRelationship;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnRelationshipGroup;

public abstract class KernelReadTracerTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private long foo;
    private long bar;
    private long bare;

    private long has;
    private long is;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        Node deleted;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node foo = graphDb.createNode( label( "Foo" ) );
            Node bar = graphDb.createNode( label( "Bar" ) );
            graphDb.createNode( label( "Baz" ) );
            graphDb.createNode( label( "Bar" ), label( "Baz" ) );
            (deleted = graphDb.createNode()).getId();
            Node bare = graphDb.createNode();

            has = foo.createRelationshipTo( bar, RelationshipType.withName( "HAS" ) ).getId();
            foo.createRelationshipTo( bar, RelationshipType.withName( "HAS" ) );
            foo.createRelationshipTo( bar, RelationshipType.withName( "IS" ) );
            foo.createRelationshipTo( bar, RelationshipType.withName( "HAS" ) );
            foo.createRelationshipTo( bar, RelationshipType.withName( "HAS" ) );

            is = bar.createRelationshipTo( bare, RelationshipType.withName( "IS" ) ).getId();

            this.foo = foo.getId();
            this.bar = bar.getId();
            this.bare = bare.getId();

            foo.setProperty( "p1", 1 );
            foo.setProperty( "p2", 2 );
            foo.setProperty( "p3", 3 );
            foo.setProperty( "p4", 4 );

            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( label( "Foo" ) ).on( "p1" ).create();
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            deleted.delete();
            tx.success();
        }
    }

    @Test
    void shouldTraceAllNodesScan()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        List<TraceEvent> expectedEvents = new ArrayList<>();
        expectedEvents.add( OnAllNodesScan );

        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            nodes.setTracer( tracer );
            read.allNodesScan( nodes );
            while ( nodes.next() )
            {
                expectedEvents.add( OnNode( nodes.nodeReference() ) );
            }
        }

        // then
        tracer.assertEvents( expectedEvents );
    }

    @Test
    void shouldTraceSingleNode()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( NodeCursor cursor = cursors.allocateNodeCursor() )
        {
            // when
            cursor.setTracer( tracer );
            read.singleNode( foo, cursor );
            cursor.next();
            tracer.assertEvents( OnNode( foo ) ).clear();

            read.singleNode( bar, cursor );
            cursor.next();
            tracer.assertEvents( OnNode( bar ) ).clear();

            read.singleNode( bare, cursor );
            cursor.next();
            tracer.assertEvents( OnNode( bare ) );
        }
    }

    @Test
    void shouldStopAndRestartTracing()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( NodeCursor cursor = cursors.allocateNodeCursor() )
        {
            // when
            cursor.setTracer( tracer );
            read.singleNode( foo, cursor );
            cursor.next();
            tracer.assertEvents( OnNode( foo ) ).clear();

            cursor.setTracer( KernelReadTracer.NONE );
            read.singleNode( bar, cursor );
            cursor.next();
            tracer.assertEvents();

            cursor.setTracer( tracer );
            read.singleNode( bare, cursor );
            cursor.next();
            tracer.assertEvents( OnNode( bare ) );
        }
    }

    @Test
    void shouldTraceLabelScan() throws KernelException
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();
        int barId = token.labelGetOrCreateForName( "Bar" );

        List<TraceEvent> expectedEvents = new ArrayList<>();
        expectedEvents.add( OnLabelScan( barId ) );

        try ( NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            cursor.setTracer( tracer );
            read.nodeLabelScan( barId, cursor );
            while ( cursor.next() )
            {
                expectedEvents.add( OnNode( cursor.nodeReference() ) );
            }
        }

        // then
        tracer.assertEvents( expectedEvents );
    }

    @Test
    void shouldTraceIndexSeek() throws KernelException
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            cursor.setTracer( tracer );
            int p1 = token.propertyKey( "p1" );
            IndexReference index = tx.schemaRead().index( token.nodeLabel( "Foo" ), p1 );
            IndexReadSession session = read.indexReadSession( index );
            read.nodeIndexSeek( session, cursor, IndexOrder.NONE, false, IndexQuery.exists( p1 ) );

            tracer.assertEvents( OnIndexSeek() ).clear();

            cursor.next();
            tracer.assertEvents( OnNode( cursor.nodeReference() ) );
        }
    }

    @Test
    void shouldTraceSingleRelationship()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( RelationshipScanCursor cursor = cursors.allocateRelationshipScanCursor() )
        {
            // when
            cursor.setTracer( tracer );
            read.singleRelationship( has, cursor );
            cursor.next();
            tracer.assertEvents( OnRelationship( has ) ).clear();

            cursor.setTracer( KernelReadTracer.NONE );
            read.singleRelationship( is, cursor );
            cursor.next();
            tracer.assertEvents();

            cursor.setTracer( tracer );
            read.singleRelationship( is, cursor );
            cursor.next();
            tracer.assertEvents( OnRelationship( is ) ).clear();

        }
    }

    @Test
    void shouldTraceRelationshipTraversal()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( RelationshipScanCursor cursor = cursors.allocateRelationshipScanCursor() )
        {
            // when
            cursor.setTracer( tracer );
            read.singleRelationship( has, cursor );
            cursor.next();
            tracer.assertEvents( OnRelationship( has ) ).clear();

            cursor.setTracer( KernelReadTracer.NONE );
            read.singleRelationship( is, cursor );
            cursor.next();
            tracer.assertEvents();

            cursor.setTracer( tracer );
            read.singleRelationship( is, cursor );
            cursor.next();
            tracer.assertEvents( OnRelationship( is ) ).clear();

        }
    }

    @Test
    void shouldTraceGroupTraversal()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( NodeCursor nodeCursor = cursors.allocateNodeCursor();
              RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor() )
        {
            // when
            groupCursor.setTracer( tracer );

            read.singleNode( foo, nodeCursor );
            nodeCursor.next();
            nodeCursor.relationships( groupCursor );

            groupCursor.next();
            int expectedType = groupCursor.type();
            tracer.assertEvents( OnRelationshipGroup( expectedType ) ).clear();

            groupCursor.setTracer( KernelReadTracer.NONE );
            groupCursor.next();
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTracePropertyAccess()
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( NodeCursor nodeCursor = cursors.allocateNodeCursor();
              PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            // when
            propertyCursor.setTracer( tracer );

            read.singleNode( foo, nodeCursor );
            nodeCursor.next();
            nodeCursor.properties( propertyCursor );

            propertyCursor.next();
            tracer.assertEvents( OnProperty( propertyCursor.propertyKey() ) ).clear();

            propertyCursor.next();
            tracer.assertEvents( OnProperty( propertyCursor.propertyKey() ) ).clear();

            propertyCursor.setTracer( KernelReadTracer.NONE );
            propertyCursor.next();
            tracer.assertEvents();

            propertyCursor.setTracer( tracer );
            propertyCursor.next();
            tracer.assertEvents( OnProperty( propertyCursor.propertyKey() ) ).clear();
        }
    }
}
