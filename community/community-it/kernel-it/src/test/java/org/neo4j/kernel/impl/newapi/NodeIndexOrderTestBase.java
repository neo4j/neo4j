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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class NodeIndexOrderTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    private final String indexName = "myIndex";

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING"} )
    void shouldRangeScanInOrder( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "hello" ) );
            nodeWithProp( tx, "bellow" );
            expected.add( nodeWithProp( tx, "schmello" ) );
            expected.add( nodeWithProp( tx, "low" ) );
            expected.add( nodeWithProp( tx, "trello" ) );
            nodeWithProp( tx, "yellow" );
            expected.add( nodeWithProp( tx, "low" ) );
            nodeWithProp( tx, "below" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "now" ) );
                expected.add( nodeWithProp( tx, "jello" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.range( prop, "hello", true, "trello", true );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING"} )
    void shouldPrefixScanInOrder( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "bee hive" ) );
            nodeWithProp( tx, "a" );
            expected.add( nodeWithProp( tx, "become" ) );
            expected.add( nodeWithProp( tx, "be" ) );
            expected.add( nodeWithProp( tx, "bachelor" ) );
            nodeWithProp( tx, "street smart" );
            expected.add( nodeWithProp( tx, "builder" ) );
            nodeWithProp( tx, "ceasar" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "bastard" ) );
                expected.add( nodeWithProp( tx, "bully" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "b" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    private void assertResultsInOrder( List<Pair<Long,Value>> expected, NodeValueIndexCursor cursor,
            IndexOrder indexOrder )
    {
        Comparator<Pair<Long,Value>> comparator = indexOrder == IndexOrder.ASCENDING ? ( a, b ) -> Values.COMPARATOR.compare( a.other(), b.other() )
                                                                                     : ( a, b ) -> Values.COMPARATOR.compare( b.other(), a.other() );

        expected.sort( comparator );
        Iterator<Pair<Long,Value>> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            Pair<Long, Value> expectedRow = expectedRows.next();
            assertThat( cursor.nodeReference() ).isEqualTo( expectedRow.first() );
            for ( int i = 0; i < cursor.numberOfProperties(); i++ )
            {
                Value value = cursor.propertyValue( i );
                assertThat( value ).isEqualTo( expectedRow.other() );
            }
        }

        assertFalse( expectedRows.hasNext() );
        assertFalse( cursor.next() );
    }

    private void createIndex()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Node" ) ).on( "prop" ).withName( indexName ).create();
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private Pair<Long,Value> nodeWithProp( KernelTransaction tx, Object value ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( "Node" ) );
        Value val = Values.of( value );
        write.nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ), val );
        return Pair.of( node, val );
    }
}
