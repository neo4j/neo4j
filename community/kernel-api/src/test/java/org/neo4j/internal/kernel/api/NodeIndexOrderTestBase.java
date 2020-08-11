/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public abstract class NodeIndexOrderTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Rule
    public final OtherThreadRule<Void> otherThreadRule = new OtherThreadRule<>();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> data()
    {
        return Arrays.asList( new Object[][]{{IndexOrder.ASCENDING}} );
    }

    @Parameterized.Parameter
    public IndexOrder indexOrder;

    @Test
    public void shouldRangeScanInOrder() throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "hello" ) );
            nodeWithProp( tx, "bellow" );
            expected.add( nodeWithProp( tx, "schmello" ) );
            expected.add( nodeWithProp( tx, "low" ) );
            expected.add( nodeWithProp( tx, "trello" ) );
            nodeWithProp( tx, "yellow" );
            expected.add( nodeWithProp( tx, "low" ) );
            nodeWithProp( tx, "below" );
            tx.success();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReference index = tx.schemaRead().index( label, prop );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "now" ) );
                expected.add( nodeWithProp( tx, "jello" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.range( prop, "hello", true, "trello", true );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );

                assertResultsInOrder( expected, cursor );
            }
        }
    }

    @Test
    public void shouldPrefixScanInOrder() throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "bee hive" ) );
            nodeWithProp( tx, "a" );
            expected.add( nodeWithProp( tx, "become" ) );
            expected.add( nodeWithProp( tx, "be" ) );
            expected.add( nodeWithProp( tx, "bachelor" ) );
            nodeWithProp( tx, "street smart" );
            expected.add( nodeWithProp( tx, "builder" ) );
            nodeWithProp( tx, "ceasar" );
            tx.success();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReference index = tx.schemaRead().index( label, prop );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "bastard" ) );
                expected.add( nodeWithProp( tx, "bully" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "b" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );

                assertResultsInOrder( expected, cursor );
            }
        }
    }

    @Test
    public void shouldNodeIndexScanInOrderWithStringInMemoryAndConcurrentUpdate() throws Exception
    {
        String a = "a";
        String b = "b";
        String c = "c";

        createIndex();

        TextValue expectedFirst = indexOrder == IndexOrder.ASCENDING ? stringValue( a ) : stringValue( c );
        TextValue expectedLast = indexOrder == IndexOrder.ASCENDING ? stringValue( c ) : stringValue( a );
        try ( Transaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( "prop" );
            int label = tx.tokenRead().nodeLabel( "Node" );
            nodeWithProp( tx, a );
            nodeWithProp( tx, c );

            IndexReference index = tx.schemaRead().index( label, prop );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {

                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ), equalTo( expectedFirst ) );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ), equalTo( expectedLast ) );

                concurrentInsert( b );

                assertFalse( cursor.next(), () -> "Did not expect to find anything more but found " + cursor.propertyValue( 0 ) );
            }
            tx.success();
        }

        // Verify we see all data in the end
        try ( Transaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( "prop" );
            int label = tx.tokenRead().nodeLabel( "Node" );
            IndexReference index = tx.schemaRead().index( label, prop );
            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
            {
                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, indexOrder, true, query );
                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ), equalTo( expectedFirst ) );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ), equalTo( stringValue( b ) ) );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ), equalTo( expectedLast ) );

                assertFalse( cursor.next() );
            }
        }
    }

    private void concurrentInsert( Object value ) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        otherThreadRule.execute( state ->
        {
            try ( Transaction otherTx = beginTransaction() )
            {
                nodeWithProp( otherTx, value );
                otherTx.success();
            }
            return null;
        } ).get();
    }

    private void assertResultsInOrder( List<Pair<Long,Value>> expected, NodeValueIndexCursor cursor )
    {
        Comparator<Pair<Long,Value>> comparator = indexOrder == IndexOrder.ASCENDING ? ( a, b ) -> Values.COMPARATOR.compare( a.other(), b.other() )
                                                                                     : ( a, b ) -> Values.COMPARATOR.compare( b.other(), a.other() );

        expected.sort( comparator );
        Iterator<Pair<Long,Value>> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            Pair<Long, Value> expectedRow = expectedRows.next();
            assertThat( cursor.nodeReference(), equalTo( expectedRow.first() ) );
            for ( int i = 0; i < cursor.numberOfProperties(); i++ )
            {
                Value value = cursor.propertyValue( i );
                assertThat( value, equalTo( expectedRow.other() ) );
            }
        }

        assertFalse( expectedRows.hasNext() );
        assertFalse( cursor.next() );
    }

    private void createIndex()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( Label.label( "Node" ) ).on( "prop" ).create();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private Pair<Long,Value> nodeWithProp( Transaction tx, Object value ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( "Node" ) );
        Value val = Values.of( value );
        write.nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ), val );
        return Pair.of( node, val );
    }
}
