/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class NodeValueIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long strOne, strTwo1, strTwo2, strThree1, strThree2, strThree3;
    private static long boolTrue, num5, num6, num12a, num12b;
    private static long strOneNoLabel;
    private static long joeDalton, williamDalton, jackDalton, averellDalton;
    private static long date891, date892, date86;

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( label( "Node" ) ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            createCompositeIndex( graphDb, "Person", "firstname", "surname" );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new AssertionError( e );
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 5, MINUTES );
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
            strOneNoLabel = nodeWithNoLabel( graphDb, "one" );
            joeDalton = person( graphDb, "Joe", "Dalton" );
            williamDalton = person( graphDb, "William", "Dalton" );
            jackDalton = person( graphDb, "Jack", "Dalton" );
            averellDalton = person( graphDb, "Averell", "Dalton" );
            nodeWithProp( graphDb, Values.pointValue( Cartesian, 1, 0 ) ); // Purposely mix order
            nodeWithProp( graphDb, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( graphDb, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( graphDb, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( graphDb, Values.pointValue( Cartesian, 0, 1 ) );
            nodeWithProp( graphDb, Values.pointValue( Cartesian_3D, 0, 0, 0 ) );
            nodeWithProp( graphDb, Values.pointValue( WGS84, 0, 0 ) );
            nodeWithProp( graphDb, Values.pointValue( WGS84_3D, 0, 0, 0 ) );
            date891 = nodeWithProp( graphDb, DateValue.date( 1989, 3, 24 ) ); // Purposely mix order
            date86 = nodeWithProp( graphDb, DateValue.date( 1986, 11, 18 ) );
            date892 = nodeWithProp( graphDb, DateValue.date( 1989, 3, 24 ) );

            tx.success();
        }
    }

    protected abstract void createCompositeIndex( GraphDatabaseService graphDb, String label, String... properties ) throws Exception;
    protected abstract String providerKey();
    protected abstract String providerVersion();
    protected abstract boolean spatialRangeSupport();

    @Test
    public void shouldPerformExactLookup() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "zero" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "two" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strTwo1, strTwo2 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "three" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 1 ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 2 ) );

            // then
            assertFoundNodesAndNoValue( node, 2, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 3 ) );

            // then
            assertFoundNodesAndNoValue( node, 3, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 6 ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, 12.0 ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, true ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, boolTrue );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, Values.pointValue( Cartesian, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 3, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, Values.pointValue( Cartesian_3D, 0, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, Values.pointValue( WGS84, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, Values.pointValue( WGS84_3D, 0, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, DateValue.date( 1989, 3, 24 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 2, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, DateValue.date( 1986, 11, 18 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );
        }
    }

    @Test
    public void shouldPerformExactLookupInCompositeIndex() throws Exception
    {
        // given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            IndexValueCapability valueCapability = index.valueCapability( ValueCategory.TEXT, ValueCategory.TEXT );
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Joe" ),
                IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertThat( node.numberOfProperties(), equalTo( 2 ) );
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, joeDalton );
        }
    }

    @Test
    public void shouldPerformStringPrefixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "t" ) );

            // then
            assertThat( node.numberOfProperties(), equalTo( 1 ) );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strTwo1, strTwo2, strThree1, strThree2,
                    strThree3 );
        }
    }

    @Test
    public void shouldPerformStringSuffixSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringSuffix( prop, "e" ) );

            // then
            assertThat( node.numberOfProperties(), equalTo( 1 ) );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    public void shouldPerformStringContainmentSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringContains( prop, "o" ) );

            // then
            assertThat( node.numberOfProperties(), equalTo( 1 ) );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformStringRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability stringCapability = index.valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

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
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, strOne, strThree1, strThree2, strThree3,
                    strTwo1, strTwo2 );
        }
    }

    @Test
    public void shouldPerformNumericRangeSearch() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability numberCapability = index.valueCapability( ValueCategory.NUMBER );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

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
    public void shouldPerformTemporalRangeSearch() throws KernelException
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability temporalCapability = index.valueCapability( ValueCategory.TEMPORAL );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, date86, date891, date892 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, date86 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), false, DateValue.date( 1989, 3, 24 ), true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, date891, date892 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), false, DateValue.date( 1989, 3, 24 ), false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability );
        }
    }

    @Test
    public void shouldPerformSpatialRangeSearch() throws KernelException
    {
        assumeTrue( spatialRangeSupport() );

        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability spatialCapability = index.valueCapability( ValueCategory.GEOMETRY );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, Cartesian ) );

            // then
            assertFoundNodesAndValue( node, 5, uniqueIds, spatialCapability );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, Cartesian_3D ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, WGS84 ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.range( prop, WGS84_3D ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability );
        }
    }

    @Test
    public void shouldPerformIndexScan() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability wildcardCapability = index.valueCapability( ValueCategory.UNKNOWN );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexScan( index, node, IndexOrder.NONE );

            // then
            assertThat( node.numberOfProperties(), equalTo( 1 ) );
            assertFoundNodesAndValue( node, 35, uniqueIds, wildcardCapability );
        }
    }

    @Test
    public void shouldRespectOrderCapabilitiesForNumbers() throws Exception
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueCategory.NUMBER );
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
        IndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueCategory.TEXT );
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
    public void shouldRespectOrderCapabilitiesForTemporal() throws KernelException
    {
        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueCategory.TEMPORAL );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability,
                        IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    public void shouldRespectOrderCapabilitiesForSpatial() throws KernelException
    {
        assumeTrue( spatialRangeSupport() );

        // given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueCategory.GEOMETRY );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, IndexQuery.range( prop, CoordinateReferenceSystem.Cartesian ) );

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
        IndexReference index = schemaRead.index( label, prop );
        IndexOrder[] orderCapabilities = index.orderCapability( ValueCategory.UNKNOWN );
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

    private void assertFoundNodesAndValue( NodeValueIndexCursor node, int nodes, MutableLongSet uniqueIds,
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

    private void assertFoundNodesAndNoValue( NodeValueIndexCursor node, int nodes, MutableLongSet uniqueIds )
    {
        uniqueIds.clear();
        for ( int i = 0; i < nodes; i++ )
        {
            assertTrue( "at least " + nodes + " nodes, was " + uniqueIds.size(), node.next() );
            long nodeReference = node.nodeReference();
            assertTrue( "all nodes are unique", uniqueIds.add( nodeReference ) );

            assertFalse( node.hasValue() );
        }

        assertFalse( "no more than " + nodes + " nodes", node.next() );
    }

    private void assertFoundNodesAndValue( NodeValueIndexCursor node, MutableLongSet uniqueIds,
            IndexValueCapability expectValue,
            long... expected )
    {
        assertFoundNodesAndValue( node, expected.length, uniqueIds, expectValue );

        for ( long expectedNode : expected )
        {
            assertTrue( "expected node " + expectedNode, uniqueIds.contains( expectedNode ) );
        }
    }

    private void assertFoundNodesAndNoValue( NodeValueIndexCursor node, MutableLongSet uniqueIds,
            long... expected )
    {
        assertFoundNodesAndNoValue( node, expected.length, uniqueIds );

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
    public void shouldGetNoIndexForMissingTokens()
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int badLabel = token.nodeLabel( "BAD_LABEL" );
        int badProp = token.propertyKey( "badProp" );

        assertEquals( "bad label", IndexReference.NO_INDEX, schemaRead.index( badLabel, prop ) );
        assertEquals( "bad prop", IndexReference.NO_INDEX, schemaRead.index( label, badProp ) );
        assertEquals( "just bad", IndexReference.NO_INDEX, schemaRead.index( badLabel, badProp ) );
    }

    @Test
    public void shouldGetNoIndexForUnknownTokens()
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int badLabel = Integer.MAX_VALUE;
        int badProp = Integer.MAX_VALUE;

        assertEquals( "bad label", IndexReference.NO_INDEX, schemaRead.index( badLabel, prop ) );
        assertEquals( "bad prop", IndexReference.NO_INDEX, schemaRead.index( label, badProp ) );
        assertEquals( "just bad", IndexReference.NO_INDEX, schemaRead.index( badLabel, badProp ) );
    }

    @Test
    public void shouldGetVersionAndKeyFromIndexReference()
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );

        assertEquals( providerKey(), index.providerKey() );
        assertEquals( providerVersion(), index.providerVersion() );
    }

    @Test
    public void shouldNotFindDeletedNodeInIndexScan() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        IndexValueCapability wildcardCapability = index.valueCapability( ValueCategory.UNKNOWN );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            tx.dataRead().nodeIndexScan( index, node, IndexOrder.NONE );
            assertThat( node.numberOfProperties(), equalTo( 1 ) );
            assertFoundNodesAndValue( node, 35, uniqueIds, wildcardCapability );

            // then
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexScan( index, node, IndexOrder.NONE );
            assertFoundNodesAndValue( node, 34, uniqueIds, wildcardCapability );
        }
    }

    @Test
    public void shouldNotFindDeletedNodeInIndexSeek() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindDNodeWithRemovedLabelInIndexSeek() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindUpdatedNodeInIndexSeek() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldFindUpdatedNodeInIndexSeek() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "ett" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    public void shouldFindSwappedNodeInIndexSeek() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( prop, "one" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    @Test
    public void shouldNotFindDeletedNodeInRangeSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataWrite().nodeDelete( strThree1 );
            tx.dataWrite().nodeDelete( strThree2 );
            tx.dataWrite().nodeDelete( strThree3 );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindNodeWithRemovedLabelInRangeSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeRemoveLabel( strThree1, label );
            tx.dataWrite().nodeRemoveLabel( strThree2, label );
            tx.dataWrite().nodeRemoveLabel( strThree3, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindUpdatedNodeInRangeSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataWrite().nodeSetProperty( strThree1, prop, stringValue( "tre" ) );
            tx.dataWrite().nodeSetProperty( strThree2, prop, stringValue( "tre" ) );
            tx.dataWrite().nodeSetProperty( strThree3, prop, stringValue( "tre" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldFindUpdatedNodeInRangeSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, "ett", true, "tre", true ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    public void shouldFindSwappedNodeInRangeSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE,
                    IndexQuery.range( prop, "one", true, "ones", true ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindDeletedNodeInPrefixSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "on" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindNodeWithRemovedLabelInPrefixSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "on" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindUpdatedNodeInPrefixSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "on" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldFindUpdatedNodeInPrefixSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "et" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    public void shouldFindSwappedNodeInPrefixSearch() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReference index = schemaRead.index( label, prop );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.stringPrefix( prop, "on" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    @Test
    public void shouldNotFindDeletedNodeInCompositeIndex() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( jackDalton );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Jack" ),
                    IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindNodeWithRemovedLabelInCompositeIndex() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( joeDalton, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Joe" ),
                    IndexQuery.exact( surname, "Dalton" ) );
            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldNotFindUpdatedNodeInCompositeIndex() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( jackDalton, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( jackDalton, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Jack" ),
                    IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    public void shouldFindUpdatedNodeInCompositeIndex() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( jackDalton, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( jackDalton, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Jesse" ),
                    IndexQuery.exact( surname, "James" ) );

            // then
            assertTrue( node.next() );
            assertEquals( jackDalton, node.nodeReference() );
        }
    }

    @Test
    public void shouldFindSwappedNodeInCompositeIndex() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReference index = schemaRead.index( label, firstName, surname );
        try ( org.neo4j.internal.kernel.api.Transaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( joeDalton, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataWrite().nodeSetProperty( strOneNoLabel, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( strOneNoLabel, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, IndexQuery.exact( firstName, "Jesse" ),
                    IndexQuery.exact( surname, "James" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    private long nodeWithProp( GraphDatabaseService graphDb, Object value )
    {
        Node node = graphDb.createNode( label( "Node" ) );
        node.setProperty( "prop", value );
        return node.getId();
    }

    private long nodeWithNoLabel( GraphDatabaseService graphDb, Object value )
    {
        Node node = graphDb.createNode();
        node.setProperty( "prop", value );
        return node.getId();
    }

    private long person( GraphDatabaseService graphDb, String firstName, String surname )
    {
        Node node = graphDb.createNode( label( "Person" ) );
        node.setProperty( "firstname", firstName );
        node.setProperty( "surname", surname );
        return node.getId();
    }
}
