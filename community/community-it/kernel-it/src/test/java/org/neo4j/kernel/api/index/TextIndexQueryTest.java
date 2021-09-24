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
package org.neo4j.kernel.api.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.text_indexes_enabled;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.schema.IndexType.TEXT;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringContains;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringPrefix;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringSuffix;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.values.storable.Values.stringValue;

public class TextIndexQueryTest extends KernelAPIReadTestBase<ReadTestSupport>
{
    private static final Label PERSON = label( "PERSON" );
    private static final RelationshipType FRIEND = RelationshipType.withName( "FRIEND" );
    private static final IndexAccessMonitor monitor = new IndexAccessMonitor();
    private static final String nodeIndexName = "some_node_text_index";
    private static final String relIndexName = "some_rel_text_index";
    private static final String NAME = "name";
    private static final String ADDRESS = "address";
    private static final String SINCE = "since";

    @BeforeEach
    void setup()
    {
        monitor.reset();
    }

    @Override
    public void createTestGraph( GraphDatabaseService db )
    {
        try ( var tx = db.beginTx() )
        {
            tx.schema().indexFor( PERSON ).on( NAME ).withName( nodeIndexName ).withIndexType( TEXT ).create();
            tx.schema().indexFor( FRIEND ).on( SINCE ).withName( relIndexName ).withIndexType( TEXT ).create();
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            var mike = tx.createNode( PERSON );
            mike.setProperty( NAME, "Mike Smith" );
            mike.setProperty( ADDRESS, "United Kingdom" );

            var james = tx.createNode( PERSON );
            james.setProperty( NAME, "James Smith" );
            james.setProperty( ADDRESS, "Heathrow, United Kingdom" );
            mike.createRelationshipTo( james, FRIEND ).setProperty( SINCE, "3 years" );

            var smith = tx.createNode( PERSON );
            smith.setProperty( NAME, "Smith James Luke" );
            smith.setProperty( ADDRESS, "United Emirates" );
            mike.createRelationshipTo( smith, FRIEND ).setProperty( SINCE, "2 years, 2 months" );
            james.createRelationshipTo( smith, FRIEND ).setProperty( SINCE, "2 years" );

            var noah = tx.createNode( PERSON );
            noah.setProperty( NAME, "Noah" );
            noah.createRelationshipTo( mike, FRIEND ).setProperty( SINCE, "4 years" );

            var alex = tx.createNode( PERSON );
            alex.setProperty( NAME, "Alex" );

            var matt = tx.createNode( PERSON );
            matt.setProperty( NAME, 42 );

            var jack = tx.createNode( PERSON );
            jack.setProperty( NAME, "77" );

            tx.commit();
        }
    }

    @Test
    void shouldFindNodes() throws Exception
    {
        assertThat( indexedNodes( exact( token.propertyKey( NAME ), "Mike Smith" ) ) ).isEqualTo( 1 );
        assertThat( indexedNodes( exact( token.propertyKey( NAME ), "Unknown" ) ) ).isEqualTo( 0 );
        assertThat( indexedNodes( exact( token.propertyKey( NAME ), 77 ) ) ).isEqualTo( 0 );
        assertThat( indexedNodes( stringPrefix( token.propertyKey( NAME ), stringValue( "Smith" ) ) ) ).isEqualTo( 1 );
        assertThat( indexedNodes( stringContains( token.propertyKey( NAME ), stringValue( "Smith" ) ) ) ).isEqualTo( 3 );
        assertThat( indexedNodes( stringSuffix( token.propertyKey( NAME ), stringValue( "Smith" ) ) ) ).isEqualTo( 2 );
        assertThat( indexedNodes( range( token.propertyKey( NAME ), "Mike Smith", true, "Noah", true ) ) ).isEqualTo( 2 );
    }

    @Test
    void shouldFindRelations() throws Exception
    {
        assertThat( indexedRelations( exact( token.propertyKey( SINCE ), "3 years" ) ) ).isEqualTo( 1 );
        assertThat( indexedRelations( exact( token.propertyKey( SINCE ), "Unknown" ) ) ).isEqualTo( 0 );
        assertThat( indexedRelations( stringContains( token.propertyKey( SINCE ), stringValue( "years" ) ) ) ).isEqualTo( 4 );
        assertThat( indexedRelations( stringSuffix( token.propertyKey( SINCE ), stringValue( "years" ) ) ) ).isEqualTo( 3 );
        assertThat( indexedRelations( stringPrefix( token.propertyKey( SINCE ), stringValue( "2 years" ) ) ) ).isEqualTo( 2 );
        assertThat( indexedRelations( range( token.propertyKey( SINCE ), "2 years", true, "3 years", true ) ) ).isEqualTo( 3 );
    }

    @Test
    void shouldRejectIndexScans()
    {
        var expectedMessage = "Index scan not supported for TEXT index";
        assertThat( assertThrows( UnsupportedOperationException.class, this::scanNodes ).getMessage() ).isEqualTo( expectedMessage );
        assertThat( assertThrows( UnsupportedOperationException.class, this::scanRelationships ).getMessage() ).isEqualTo( expectedMessage );
    }

    private void scanNodes() throws Exception
    {
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( nodeIndexName ) );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE ) )
        {
            read.nodeIndexScan( index, cursor, unconstrained() );
        }
    }

    private void scanRelationships() throws Exception
    {
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( relIndexName ) );
        try ( RelationshipValueIndexCursor cursor = cursors.allocateRelationshipValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE ) )
        {
            read.relationshipIndexScan( index, cursor, unconstrained() );
        }
    }

    private long indexedNodes( PropertyIndexQuery... query ) throws Exception
    {
        monitor.reset();
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( nodeIndexName ) );
        try ( NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE ) )
        {
            read.nodeIndexSeek( tx.queryContext(), index, cursor, unconstrained(), query );
            assertThat( monitor.accessed( org.neo4j.internal.schema.IndexType.TEXT ) ).isEqualTo( 1 );
            return count( cursor );
        }
    }

    private long indexedRelations( PropertyIndexQuery query ) throws Exception
    {
        monitor.reset();
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( relIndexName ) );
        try ( RelationshipValueIndexCursor cursor = cursors.allocateRelationshipValueIndexCursor( NULL, EmptyMemoryTracker.INSTANCE ) )
        {
            read.relationshipIndexSeek( tx.queryContext(), index, cursor, unconstrained(), query );
            assertThat( monitor.accessed( org.neo4j.internal.schema.IndexType.TEXT ) ).isEqualTo( 1 );
            return count( cursor );
        }
    }

    private static class IndexAccessMonitor extends IndexMonitor.MonitorAdapter
    {
        private final Map<IndexType,Integer> counts = new HashMap<>();

        @Override
        public void queried( IndexDescriptor descriptor )
        {
            counts.putIfAbsent( descriptor.getIndexType(), 0 );
            counts.computeIfPresent( descriptor.getIndexType(), ( type, value ) -> value + 1 );
        }

        public int accessed( org.neo4j.internal.schema.IndexType type )
        {
            return counts.getOrDefault( type, 0 );
        }

        void reset()
        {
            counts.clear();
        }
    }

    @Override
    public ReadTestSupport newTestSupport()
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        ReadTestSupport support = new ReadTestSupport();
        support.addSetting( text_indexes_enabled, true );
        support.setMonitors( monitors );
        return support;
    }

    private long count( Cursor cursor )
    {
        int result = 0;
        while ( cursor.next() )
        {
            result++;
        }
        return result;
    }
}
