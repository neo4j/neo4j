/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.common.EntityType;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public abstract class DefaultPooledCursorsTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static long startNode, relationship, propNode;
    private static final String NODE_PROP_INDEX_NAME = "nodeProp";

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "Node" ) ).on( "prop" ).withName( NODE_PROP_INDEX_NAME ).create();
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node a = tx.createNode( label( "Foo" ) );
            Node b = tx.createNode( label( "Bar" ) );
            startNode = a.getId();
            relationship = a.createRelationshipTo( b, withName( "REL" ) ).getId();
            propNode = createNodeWithProperty( tx, "prop", true );
            tx.commit();
        }
    }

    @Test
    void shouldReuseNodeCursor()
    {
        NodeCursor c1 = cursors.allocateNodeCursor( NULL );
        read.singleNode( startNode, c1 );
        c1.close();

        NodeCursor c2 = cursors.allocateNodeCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseFullAccessNodeCursor()
    {
        NodeCursor c1 = cursors.allocateFullAccessNodeCursor( NULL );
        read.singleNode( startNode, c1 );
        c1.close();

        NodeCursor c2 = cursors.allocateFullAccessNodeCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseRelationshipScanCursor()
    {
        RelationshipScanCursor c1 = cursors.allocateRelationshipScanCursor( NULL );
        read.singleRelationship( relationship, c1 );
        c1.close();

        RelationshipScanCursor c2 = cursors.allocateRelationshipScanCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseFullAccessRelationshipScanCursor()
    {
        RelationshipScanCursor c1 = cursors.allocateFullAccessRelationshipScanCursor( NULL );
        read.singleRelationship( relationship, c1 );
        c1.close();

        RelationshipScanCursor c2 = cursors.allocateFullAccessRelationshipScanCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseRelationshipTraversalCursor()
    {
        NodeCursor node = cursors.allocateNodeCursor( NULL );
        RelationshipTraversalCursor c1 = cursors.allocateRelationshipTraversalCursor( NULL );

        read.singleNode( startNode, node );
        node.next();
        node.relationships( c1, ALL_RELATIONSHIPS );

        node.close();
        c1.close();

        RelationshipTraversalCursor c2 = cursors.allocateRelationshipTraversalCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseFullAccessRelationshipTraversalCursor()
    {
        NodeCursor node = cursors.allocateNodeCursor( NULL );
        RelationshipTraversalCursor c1 = cursors.allocateFullAccessRelationshipTraversalCursor( NULL );

        read.singleNode( startNode, node );
        node.next();
        node.relationships( c1, ALL_RELATIONSHIPS );

        node.close();
        c1.close();

        RelationshipTraversalCursor c2 = cursors.allocateFullAccessRelationshipTraversalCursor( NULL );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReusePropertyCursor()
    {
        NodeCursor node = cursors.allocateNodeCursor( NULL );
        PropertyCursor c1 = cursors.allocatePropertyCursor( NULL, INSTANCE );

        read.singleNode( propNode, node );
        node.next();
        node.properties( c1 );

        node.close();
        c1.close();

        PropertyCursor c2 = cursors.allocatePropertyCursor( NULL, INSTANCE );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseFullAccessPropertyCursor()
    {
        NodeCursor node = cursors.allocateNodeCursor( NULL );
        PropertyCursor c1 = cursors.allocateFullAccessPropertyCursor( NULL, INSTANCE );

        read.singleNode( propNode, node );
        node.next();
        node.properties( c1 );

        node.close();
        c1.close();

        PropertyCursor c2 = cursors.allocateFullAccessPropertyCursor( NULL, INSTANCE );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseNodeValueIndexCursor() throws Exception
    {
        int prop = token.propertyKey( "prop" );
        IndexDescriptor indexDescriptor = tx.schemaRead().indexGetForName( NODE_PROP_INDEX_NAME );
        Predicates.awaitEx( () -> tx.schemaRead().indexGetState( indexDescriptor ) == ONLINE, 1, MINUTES );
        IndexReadSession indexSession = tx.dataRead().indexReadSession( indexDescriptor );

        NodeValueIndexCursor c1 = cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
        read.nodeIndexSeek( indexSession, c1, IndexQueryConstraints.unconstrained(), PropertyIndexQuery.exact( prop, "zero" ) );
        c1.close();

        NodeValueIndexCursor c2 = cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseFullAccessNodeValueIndexCursor() throws Exception
    {
        int prop = token.propertyKey( "prop" );
        IndexDescriptor indexDescriptor = tx.schemaRead().indexGetForName( NODE_PROP_INDEX_NAME );
        Predicates.awaitEx( () -> tx.schemaRead().indexGetState( indexDescriptor ) == ONLINE, 1, MINUTES );
        IndexReadSession indexSession = tx.dataRead().indexReadSession( indexDescriptor );

        NodeValueIndexCursor c1 = cursors.allocateFullAccessNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
        read.nodeIndexSeek( indexSession, c1, IndexQueryConstraints.unconstrained(), PropertyIndexQuery.exact( prop, "zero" ) );
        c1.close();

        NodeValueIndexCursor c2 = cursors.allocateFullAccessNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE);
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    @Test
    void shouldReuseNodeLabelIndexCursor() throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            NodeLabelIndexCursor c1 = tx.cursors().allocateNodeLabelIndexCursor( NULL );
            tx.dataRead().nodeLabelScan( 1, c1, IndexOrder.NONE );
            c1.close();

            NodeLabelIndexCursor c2 = tx.cursors().allocateNodeLabelIndexCursor( NULL );
            assertThat( c1 ).isSameAs( c2 );
            c2.close();
        }
    }

    @Test
    void shouldReuseFullAccessNodeLabelIndexCursor() throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            NodeLabelIndexCursor c1 = tx.cursors().allocateFullAccessNodeLabelIndexCursor( NULL );
            tx.dataRead().nodeLabelScan( 1, c1, IndexOrder.NONE );
            c1.close();

            NodeLabelIndexCursor c2 = tx.cursors().allocateFullAccessNodeLabelIndexCursor( NULL );
            assertThat( c1 ).isSameAs( c2 );
            c2.close();
        }
    }

    @Test
    void shouldReuseRelationshipIndexCursors() throws Exception
    {
        // given
        int connection;
        int name;
        String indexName = "myIndex";
        IndexDescriptor index;

        try ( KernelTransaction tx = beginTransaction() )
        {
            connection = tx.tokenWrite().relationshipTypeGetOrCreateForName( "Connection" );
            name = tx.tokenWrite().propertyKeyGetOrCreateForName( "name" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            SchemaDescriptor schema = SchemaDescriptor.fulltext( EntityType.RELATIONSHIP, array( connection ), array( name ) );
            IndexPrototype prototype = IndexPrototype.forSchema( schema, DESCRIPTOR ).withName( indexName ).withIndexType( IndexType.FULLTEXT );
            index = tx.schemaWrite().indexCreate( prototype );
            tx.commit();
        }

        Predicates.awaitEx( () -> tx.schemaRead().indexGetState( index ) == ONLINE, 1, MINUTES );

        RelationshipValueIndexCursor c1 = cursors.allocateRelationshipValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE );
        IndexReadSession indexSession = tx.dataRead().indexReadSession( index );
        read.relationshipIndexSeek( indexSession, c1, IndexQueryConstraints.unconstrained(), PropertyIndexQuery.fulltextSearch( "hello" ) );
        c1.close();

        RelationshipValueIndexCursor c2 = cursors.allocateRelationshipValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE );
        assertThat( c1 ).isSameAs( c2 );
        c2.close();
    }

    private int[] array( int... elements )
    {
        return elements;
    }

    private long createNodeWithProperty( Transaction tx, String propertyKey, Object value )
    {
        Node p = tx.createNode();
        p.setProperty( propertyKey, value );
        return p.getId();
    }
}
