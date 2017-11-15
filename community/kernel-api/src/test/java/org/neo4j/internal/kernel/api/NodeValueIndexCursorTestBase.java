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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

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
            graphDb.schema().awaitIndexOnline( index, 60, SECONDS );
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
            nodeWithProp( graphDb, 3 ); // Purposely mix ordering
            nodeWithProp( graphDb, 3 );
            nodeWithProp( graphDb, 3 );
            nodeWithProp( graphDb, 2 );
            nodeWithProp( graphDb, 2 );
            nodeWithProp( graphDb, 1 );
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
        CapableIndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            IndexValueCapability valueCapability = index.valueCapability( ValueGroup.TEXT );
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "zero" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "two" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, strTwo1, strTwo2 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "three" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, strThree1, strThree2, strThree3 );

            // when
            valueCapability = index.valueCapability( ValueGroup.NUMBER );
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 1 ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, valueCapability );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 2 ) );

            // then
            assertFoundNodesAndValue( node, 2, uniqueIds, valueCapability );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 3 ) );

            // then
            assertFoundNodesAndValue( node, 3, uniqueIds, valueCapability );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 6 ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 12.0 ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, num12a, num12b );

            // when
            valueCapability = index.valueCapability( ValueGroup.BOOLEAN );
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, boolTrue );
        }
    }

    @Test
    public void shouldPerformStringPrefixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueGroup.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "t" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strTwo1, strTwo2, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    public void shouldPerformStringSuffixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueGroup.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringSuffix( prop, "e" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    public void shouldPerformStringContainmentSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueGroup.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringContains( prop, "o" ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformStringRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueGroup.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, "one", true, "three", true ) );

            // then

            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, "one", true, "three", false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, "one", false, "three", true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, "one", false, "two", false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, "one", true, "two", true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strThree1, strThree2, strThree3, strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformNumericRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability numberCapability = index.valueCapability( ValueGroup.NUMBER );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, 5, true, 12, true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, num5, num6, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, 5, true, 12, false ) );

            // then

            assertFoundNodesAndValue( node, uniqueIds, numberCapability, num5, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, 5, false, 12, true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, num6, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, 5, false, 12, false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, num6 );
        }
    }

    @Test
    public void shouldPerformIndexScan() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexValueCapability wildcardCapability = index.valueCapability( ValueGroup.UNKNOWN );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor();
              PrimitiveLongSet uniqueIds = Primitive.longSet() )
        {
            // when
            read.nodeIndexScan( index, node, IndexOrder.NONE );

            // then
            assertFoundNodesAndValue( node, 24, uniqueIds, wildcardCapability );
        }
    }

    @Test
    public void shouldRespectOrderCapabilitiesForNumbers() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueGroup.NUMBER );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, IndexQuery.range( prop, 1, true, 42, true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    public void shouldRespectOrderCapabilitiesForStrings() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueGroup.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, IndexQuery.range( prop, "one", true, "two", true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    public void shouldRespectOrderCapabilitiesForWildcard() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        CapableIndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueGroup.UNKNOWN );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, IndexQuery.exists( prop ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    private void assertFoundNodesInOrder( NodeValueIndexCursor node, IndexOrder indexOrder )
    {
        Value currentValue = null;
        while ( node.next() )
        {
            long nodeReference = node.nodeReference();
            Value storedValue = getPropertyValueFromStore( nodeReference );
            if ( currentValue != null )
            {
                switch ( indexOrder )
                {
                case ASCENDING:
                    assertTrue( "Requested ordering " + indexOrder + " was not respected.",
                            Values.COMPARATOR.compare( currentValue, storedValue ) <= 0 );
                    break;
                case DESCENDING:
                    assertTrue( "Requested ordering " + indexOrder + " was not respected.",
                            Values.COMPARATOR.compare( currentValue, storedValue ) >= 0 );
                    break;
                case NONE:
                    // Don't verify
                    break;
                default:
                    throw new UnsupportedOperationException( "Can not verify ordering for " + indexOrder );
                }
            }
            currentValue = storedValue;
        }
    }

    private void assertFoundNodesAndValue( NodeValueIndexCursor node, int nodes, PrimitiveLongSet uniqueIds,
            IndexValueCapability expectValue )
    {
        uniqueIds.clear();
        for ( int i = 0; i < nodes; i++ )
        {
            assertTrue( "at least " + nodes + " nodes, was " + uniqueIds.size(), node.next() );
            long nodeReference = node.nodeReference();
            assertTrue( "all nodes are unique", uniqueIds.add( nodeReference ) );

            // Assert has value capability
            if ( IndexValueCapability.YES.equals( expectValue ) )
            {
                assertTrue( "has value", node.hasValue() );
            }

            // Assert has correct value
            if ( node.hasValue() )
            {
                Value storedValue = getPropertyValueFromStore( nodeReference );
                assertThat( "has correct value", node.propertyValue( 0 ), is( storedValue ) );
            }
        }

        assertFalse( "no more than " + nodes + " nodes", node.next() );
    }

    private void assertFoundNodesAndValue( NodeValueIndexCursor node, PrimitiveLongSet uniqueIds, IndexValueCapability expectValue,
            long... expected )
    {
        assertFoundNodesAndValue( node, expected.length, uniqueIds, expectValue );

        for ( long expectedNode : expected )
        {
            assertTrue( "expected node " + expectedNode, uniqueIds.contains( expectedNode ) );
        }
    }

    private Value getPropertyValueFromStore( long nodeReference )
    {
        try ( NodeCursor storeCursor = cursors.allocateNodeCursor();
              PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            read.singleNode( nodeReference, storeCursor );
            storeCursor.next();
            storeCursor.properties( propertyCursor );
            propertyCursor.next();
            return propertyCursor.propertyValue();
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
