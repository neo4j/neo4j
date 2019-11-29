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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class NodeValueIndexCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static final int TOTAL_NODE_COUNT = 37;
    private static final String COMPOSITE_INDEX_NAME = "compositeIndex";
    private static final String NODE_PROP_INDEX_NAME = "nodeProp";
    private static final String NODE_PROP_2_INDEX_NAME = "nodeProp2";
    private static final String NODE_PROP_3_INDEX_NAME = "nodeProp3";
    private static final String WHAT_EVER_INDEX_NAME = "whatEver";
    private static long strOne, strTwo1, strTwo2, strThree1, strThree2, strThree3;
    private static long boolTrue, num5, num6, num12a, num12b;
    private static long strOneNoLabel;
    private static long joeDalton, williamDalton, jackDalton, averellDalton;
    private static long date891, date892, date86;
    private static long[] nodesOfAllPropertyTypes;
    private static long whateverPoint;

    private static final PointValue POINT_1 =
            PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, crs: 'wgs-84'}" );
    private static final PointValue POINT_2 =
            PointValue.parse( "{latitude: 40.7128, longitude: -74.006000001, crs: 'wgs-84'}" );

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "Node" ) ).on( "prop" ).withName( NODE_PROP_INDEX_NAME ).create();
            tx.schema().indexFor( label( "Node" ) ).on( "prop2" ).withName( NODE_PROP_2_INDEX_NAME ).create();
            tx.schema().indexFor( label( "Node" ) ).on( "prop3" ).withName( NODE_PROP_3_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "What" ) ).on( "ever" ).withName( WHAT_EVER_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( label( "Person" ) ).on( "firstname" ).on( "surname" ).withName( COMPOSITE_INDEX_NAME ).create();
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new AssertionError( e );
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, MINUTES );
            tx.commit();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            strOne = nodeWithProp( tx, "one" );
            strTwo1 = nodeWithProp( tx, "two" );
            strTwo2 = nodeWithProp( tx, "two" );
            strThree1 = nodeWithProp( tx, "three" );
            strThree2 = nodeWithProp( tx, "three" );
            strThree3 = nodeWithProp( tx, "three" );
            nodeWithProp( tx, false );
            boolTrue = nodeWithProp( tx, true );
            nodeWithProp( tx, 3 ); // Purposely mix ordering
            nodeWithProp( tx, 3 );
            nodeWithProp( tx, 3 );
            nodeWithProp( tx, 2 );
            nodeWithProp( tx, 2 );
            nodeWithProp( tx, 1 );
            nodeWithProp( tx, 4 );
            num5 = nodeWithProp( tx, 5 );
            num6 = nodeWithProp( tx, 6 );
            num12a = nodeWithProp( tx, 12.0 );
            num12b = nodeWithProp( tx, 12.0 );
            nodeWithProp( tx, 18 );
            nodeWithProp( tx, 24 );
            nodeWithProp( tx, 30 );
            nodeWithProp( tx, 36 );
            nodeWithProp( tx, 42 );
            strOneNoLabel = nodeWithNoLabel( tx, "one" );
            joeDalton = person( tx, "Joe", "Dalton" );
            williamDalton = person( tx, "William", "Dalton" );
            jackDalton = person( tx, "Jack", "Dalton" );
            averellDalton = person( tx, "Averell", "Dalton" );
            nodeWithProp( tx, Values.pointValue( Cartesian, 1, 0 ) ); // Purposely mix order
            nodeWithProp( tx, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( tx, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( tx, Values.pointValue( Cartesian, 0, 0 ) );
            nodeWithProp( tx, Values.pointValue( Cartesian, 0, 1 ) );
            nodeWithProp( tx, Values.pointValue( Cartesian_3D, 0, 0, 0 ) );
            nodeWithProp( tx, Values.pointValue( WGS84, 0, 0 ) );
            nodeWithProp( tx, Values.pointValue( WGS84_3D, 0, 0, 0 ) );
            date891 = nodeWithProp( tx, DateValue.date( 1989, 3, 24 ) ); // Purposely mix order
            date86 = nodeWithProp( tx, DateValue.date( 1986, 11, 18 ) );
            date892 = nodeWithProp( tx, DateValue.date( 1989, 3, 24 ) );
            nodeWithProp( tx, new String[]{"first", "second", "third"} );
            nodeWithProp( tx, new String[]{"fourth", "fifth", "sixth", "seventh"} );

            MutableLongList listOfIds = LongLists.mutable.empty();
            listOfIds.add(nodeWithWhatever( tx, "string" ));
            listOfIds.add(nodeWithWhatever( tx, false ));
            listOfIds.add(nodeWithWhatever( tx, 3 ));
            listOfIds.add(nodeWithWhatever( tx, 13.0 ));
            whateverPoint = nodeWithWhatever( tx, Values.pointValue( Cartesian, 1, 0 ) );
            listOfIds.add( whateverPoint );
            listOfIds.add(nodeWithWhatever( tx, DateValue.date( 1989, 3, 24 ) ));
            listOfIds.add(nodeWithWhatever( tx, new String[]{"first", "second", "third"} ));

            nodesOfAllPropertyTypes = listOfIds.toArray();

            assertSameDerivedValue( POINT_1, POINT_2 );
            nodeWithProp( tx, "prop3", POINT_1.asObjectCopy() );
            nodeWithProp( tx, "prop3", POINT_2.asObjectCopy() );
            nodeWithProp( tx, "prop3", POINT_2.asObjectCopy() );

            tx.commit();
        }
    }

    protected abstract String providerKey();
    protected abstract String providerVersion();

    protected boolean indexProvidesStringValues()
    {
        return false;
    }

    protected boolean indexProvidesNumericValues()
    {
        return false;
    }

    protected boolean indexProvidesArrayValues()
    {
        return false;
    }

    protected boolean indexProvidesBooleanValues()
    {
        return false;
    }

    protected boolean indexProvidesTemporalValues()
    {
        return true;
    }
    protected abstract void assertSameDerivedValue( PointValue p1, PointValue p2 );

    protected boolean indexProvidesSpatialValues()
    {
        return false;
    }

    protected boolean indexProvidesAllValues()
    {
        return false;
    }

    @Test
    void shouldPerformExactLookup() throws Exception
    {
        // given
        boolean needsValues = false;
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "zero" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "one" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "two" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strTwo1, strTwo2 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "three" ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, 1 ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, 2 ) );

            // then
            assertFoundNodesAndNoValue( node, 2, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, 3 ) );

            // then
            assertFoundNodesAndNoValue( node, 3, uniqueIds );

            //when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, 6 ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, 12.0 ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, true ) );

            // then
            assertFoundNodesAndNoValue( node, uniqueIds, boolTrue );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, Values.pointValue( Cartesian, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 3, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, Values.pointValue( Cartesian_3D, 0, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, Values.pointValue( WGS84, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, Values.pointValue( WGS84_3D, 0, 0, 0 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, DateValue.date( 1989, 3, 24 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 2, uniqueIds );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, DateValue.date( 1986, 11, 18 ) ) );

            // then
            assertFoundNodesAndNoValue( node, 1, uniqueIds );
        }
    }

    @Test
    void shouldPerformExactLookupInCompositeIndex() throws Exception
    {
        // given
        boolean needsValues = false;
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Joe" ),
                IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertThat( node.numberOfProperties() ).isEqualTo( 2 );
            assertFoundNodesAndNoValue( node, 1, uniqueIds );
        }
    }

    @Test
    void shouldPerformStringPrefixSearch() throws Exception
    {
        // given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability stringCapability = index.reference().getCapability().valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "t" ) ) );

            // then
            assertThat( node.numberOfProperties() ).isEqualTo( 1 );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strTwo1, strTwo2, strThree1, strThree2,
                    strThree3 );
        }
    }

    @Test
    void shouldPerformStringSuffixSearch() throws Exception
    {
        // given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability stringCapability = index.reference().getCapability().valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringSuffix( prop, stringValue( "e" ) ) );

            // then
            assertThat( node.numberOfProperties() ).isEqualTo( 1 );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strOne, strThree1, strThree2, strThree3 );
        }
    }

    @Test
    void shouldPerformStringContainmentSearch() throws Exception
    {
        // given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability stringCapability = index.reference().getCapability().valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringContains( prop, stringValue( "o" ) ) );

            // then
            assertThat( node.numberOfProperties() ).isEqualTo( 1 );
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strOne, strTwo1, strTwo2 );
        }
    }

    @Test
    void shouldPerformStringRangeSearch() throws Exception
    {
        // given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability stringCapability = index.reference().getCapability().valueCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "three", true ) );

            // then

            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strOne, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "three", false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strOne );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", false, "three", true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", false, "two", false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strThree1, strThree2, strThree3 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "two", true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, stringCapability, needsValues, strOne, strThree1, strThree2, strThree3,
                    strTwo1, strTwo2 );
        }
    }

    @Test
    void shouldPerformNumericRangeSearch() throws Exception
    {
        // given
        boolean needsValues = indexProvidesNumericValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability numberCapability = index.reference().getCapability().valueCapability( ValueCategory.NUMBER );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, 5, true, 12, true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, needsValues, num5, num6, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, 5, true, 12, false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, needsValues, num5, num6 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, 5, false, 12, true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, needsValues, num6, num12a, num12b );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, 5, false, 12, false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, numberCapability, needsValues, num6 );
        }
    }

    @Test
    void shouldPerformTemporalRangeSearch() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesTemporalValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability temporalCapability = index.reference().getCapability().valueCapability( ValueCategory.TEMPORAL );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, needsValues, date86, date891, date892 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, needsValues, date86 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), false, DateValue.date( 1989, 3, 24 ), true ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, needsValues, date891, date892 );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues,
                    IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), false, DateValue.date( 1989, 3, 24 ), false ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, temporalCapability, needsValues );
        }
    }

    @Test
    void shouldPerformSpatialRangeSearch() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesSpatialValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability spatialCapability = index.reference().getCapability().valueCapability( ValueCategory.GEOMETRY );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, Cartesian ) );

            // then
            assertFoundNodesAndValue( node, 5, uniqueIds, spatialCapability, needsValues );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, Cartesian_3D ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability, needsValues );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, WGS84 ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability, needsValues );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, WGS84_3D ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, spatialCapability, needsValues );
        }
    }

    @Test
    void shouldPerformBooleanSearch() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesBooleanValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability capability = index.reference().getCapability().valueCapability( ValueGroup.BOOLEAN.category() );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, false ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, capability, needsValues );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, true ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, capability, needsValues );
        }
    }

    @Test
    void shouldPerformTextArraySearch() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesArrayValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability capability = index.reference().getCapability().valueCapability( ValueGroup.TEXT_ARRAY.category() );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, new String[]{"first", "second", "third"} ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, capability, needsValues );

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, new String[]{"fourth", "fifth", "sixth", "seventh"} ) );

            // then
            assertFoundNodesAndValue( node, 1, uniqueIds, capability, needsValues );
        }
    }

    @Test
    void shouldPerformIndexScan() throws Exception
    {
        // given
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability wildcardCapability = index.reference().getCapability().valueCapability( ValueCategory.UNKNOWN );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexScan( index, node, IndexOrder.NONE, indexProvidesAllValues() );

            // then
            assertThat( node.numberOfProperties() ).isEqualTo( 1 );
            assertFoundNodesAndValue( node, TOTAL_NODE_COUNT, uniqueIds, wildcardCapability, indexProvidesAllValues() );
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForNumbers() throws Exception
    {
        // given
        boolean needsValues = indexProvidesNumericValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.NUMBER );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues, IndexQuery.range( prop, 1, true, 42, true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForStrings() throws Exception
    {
        // given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.TEXT );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues, IndexQuery.range( prop, "one", true, "two", true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForTemporal() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesTemporalValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.TEMPORAL );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues,
                        IndexQuery.range( prop, DateValue.date( 1986, 11, 18 ), true, DateValue.date( 1989, 3, 24 ), true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForSpatial() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesSpatialValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.GEOMETRY );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues, IndexQuery.range( prop, CoordinateReferenceSystem.Cartesian ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForStringArray() throws KernelException
    {
        // given
        boolean needsValues = indexProvidesSpatialValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.TEXT_ARRAY );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues, IndexQuery.range( prop,
                        Values.of( new String[]{"first", "second", "third"} ), true,
                        Values.of( new String[]{"fourth", "fifth", "sixth", "seventh"} ), true ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForWildcard() throws Exception
    {
        // given
        boolean needsValues = false;
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexOrder[] orderCapabilities = index.reference().getCapability().orderCapability( ValueCategory.UNKNOWN );
        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            for ( IndexOrder orderCapability : orderCapabilities )
            {
                // when
                read.nodeIndexSeek( index, node, orderCapability, needsValues, IndexQuery.exists( prop ) );

                // then
                assertFoundNodesInOrder( node, orderCapability );
            }
        }
    }

    @Test
    void shouldProvideValuesForPoints() throws Exception
    {
        // given
        assumeTrue( indexProvidesSpatialValues() );

        int prop = token.propertyKey( "ever" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( WHAT_EVER_INDEX_NAME ) );
        assertEquals( IndexValueCapability.YES, index.reference().getCapability().valueCapability( ValueCategory.GEOMETRY ) );

        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, true, IndexQuery.range( prop, Cartesian ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, index.reference().getCapability().valueCapability( ValueCategory.GEOMETRY ), true, whateverPoint );
        }
    }

    @Test
    void shouldProvideValuesForAllTypes() throws Exception
    {
        // given
        assumeTrue( indexProvidesAllValues() );

        int prop = token.propertyKey( "ever" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( WHAT_EVER_INDEX_NAME ) );
        IndexValueCapability valueCapability = index.reference().getCapability().valueCapability( ValueCategory.UNKNOWN );
        assertEquals( IndexValueCapability.YES, valueCapability );

        try ( NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            read.nodeIndexSeek( index, node, IndexOrder.NONE, true, IndexQuery.exists( prop ) );

            // then
            assertFoundNodesAndValue( node, uniqueIds, valueCapability, true, nodesOfAllPropertyTypes );
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
                    assertTrue( Values.COMPARATOR.compare( currentValue, storedValue ) <= 0,
                            "Requested ordering " + indexOrder + " was not respected."
                    );
                    break;
                case DESCENDING:
                    assertTrue( Values.COMPARATOR.compare( currentValue, storedValue ) >= 0,
                            "Requested ordering " + indexOrder + " was not respected."
                    );
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

    private void assertFoundNodesAndValue( NodeValueIndexCursor node,
                                           int nodes,
                                           MutableLongSet uniqueIds,
                                           IndexValueCapability expectValue,
                                           boolean indexProvidesValues )
    {
        uniqueIds.clear();
        for ( int i = 0; i < nodes; i++ )
        {
            assertTrue( node.next(), "at least " + nodes + " nodes, was " + uniqueIds.size() );
            long nodeReference = node.nodeReference();
            assertTrue( uniqueIds.add( nodeReference ), "all nodes are unique" );

            // Assert has value capability
            if ( IndexValueCapability.YES.equals( expectValue ) )
            {
                assertTrue( node.hasValue(),
                        "Value capability said index would have value for " + expectValue + ", but didn't" );
            }

            // Assert has correct value
            if ( indexProvidesValues )
            {
                assertTrue( node.hasValue(), "Index did not provide values" );
                Value storedValue = getPropertyValueFromStore( nodeReference );
                assertThat( node.propertyValue( 0 ) ).as( "has correct value" ).isEqualTo( storedValue );
            }
        }

        assertFalse( node.next(), "no more than " + nodes + " nodes" );
    }

    private void assertFoundNodesAndNoValue( NodeValueIndexCursor node, int nodes, MutableLongSet uniqueIds )
    {
        uniqueIds.clear();
        for ( int i = 0; i < nodes; i++ )
        {
            assertTrue( node.next(), "at least " + nodes + " nodes, was " + uniqueIds.size() );
            long nodeReference = node.nodeReference();
            assertTrue( uniqueIds.add( nodeReference ), "all nodes are unique" );

            // We can't quite assert !node.hasValue() because even tho pure SpatialIndexReader is guaranteed to not return any values,
            // where null could be used, the generic native index, especially when having composite keys including spatial values it's
            // more of a gray area and some keys may be spatial, some not and therefore a proper Value[] will be extracted
            // potentially containing some NO_VALUE values.
        }

        assertFalse( node.next(), "no more than " + nodes + " nodes" );
    }

    private void assertFoundNodesAndValue( NodeValueIndexCursor node, MutableLongSet uniqueIds, IndexValueCapability expectValue, boolean indexProvidesValues,
            long... expected )
    {
        assertFoundNodesAndValue( node, expected.length, uniqueIds, expectValue, indexProvidesValues );

        for ( long expectedNode : expected )
        {
            assertTrue( uniqueIds.contains( expectedNode ), "expected node " + expectedNode );
        }
    }

    private void assertFoundNodesAndNoValue( NodeValueIndexCursor node, MutableLongSet uniqueIds,
            long... expected )
    {
        assertFoundNodesAndNoValue( node, expected.length, uniqueIds );

        for ( long expectedNode : expected )
        {
            assertTrue( uniqueIds.contains( expectedNode ), "expected node " + expectedNode );
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
    void shouldGetNoIndexForMissingTokens()
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int badLabel = label + 1000;
        int badProp = prop + 1000;

        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( badLabel, prop ) ).hasNext(), "bad label" );
        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( label, badProp ) ).hasNext(), "bad prop" );
        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( badLabel, badProp ) ).hasNext(), "just bad" );
    }

    @Test
    void shouldGetNoIndexForUnknownTokens()
    {
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        int badLabel = Integer.MAX_VALUE;
        int badProp = Integer.MAX_VALUE;

        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( badLabel, prop ) ).hasNext(), "bad label" );
        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( label, badProp ) ).hasNext(), "bad prop" );
        assertFalse( schemaRead.index( SchemaDescriptor.forLabel( badLabel, badProp ) ).hasNext(), "just bad" );
    }

    @Test
    void shouldGetVersionAndKeyFromIndexReference()
    {
        // Given
        IndexDescriptor index = schemaRead.indexGetForName( NODE_PROP_INDEX_NAME );

        assertEquals( providerKey(), index.getIndexProvider().getKey() );
        assertEquals( providerVersion(), index.getIndexProvider().getVersion() );
    }

    @Test
    void shouldNotFindDeletedNodeInIndexScan() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesAllValues();
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        IndexValueCapability wildcardCapability = index.reference().getCapability().valueCapability( ValueCategory.UNKNOWN );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            tx.dataRead().nodeIndexScan( index, node, IndexOrder.NONE, needsValues );
            assertThat( node.numberOfProperties() ).isEqualTo( 1 );
            assertFoundNodesAndValue( node, TOTAL_NODE_COUNT, uniqueIds, wildcardCapability, needsValues );

            // then
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexScan( index, node, IndexOrder.NONE, needsValues );
            assertFoundNodesAndValue( node, TOTAL_NODE_COUNT - 1, uniqueIds, wildcardCapability, needsValues );
        }
    }

    @Test
    void shouldNotFindDeletedNodeInIndexSeek() throws Exception
    {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindDNodeWithRemovedLabelInIndexSeek() throws Exception
    {
        // Given
        boolean needsValues = false;
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindUpdatedNodeInIndexSeek() throws Exception
    {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "one" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldFindUpdatedNodeInIndexSeek() throws Exception
    {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "ett" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    void shouldFindSwappedNodeInIndexSeek() throws Exception
    {
        // Given
        boolean needsValues = false;
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( prop, "one" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    @Test
    void shouldNotFindDeletedNodeInRangeSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataWrite().nodeDelete( strThree1 );
            tx.dataWrite().nodeDelete( strThree2 );
            tx.dataWrite().nodeDelete( strThree3 );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindNodeWithRemovedLabelInRangeSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeRemoveLabel( strThree1, label );
            tx.dataWrite().nodeRemoveLabel( strThree2, label );
            tx.dataWrite().nodeRemoveLabel( strThree3, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindUpdatedNodeInRangeSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataWrite().nodeSetProperty( strThree1, prop, stringValue( "tre" ) );
            tx.dataWrite().nodeSetProperty( strThree2, prop, stringValue( "tre" ) );
            tx.dataWrite().nodeSetProperty( strThree3, prop, stringValue( "tre" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "three", true ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldFindUpdatedNodeInRangeSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "ett", true, "tre", true ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    void shouldFindSwappedNodeInRangeSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.range( prop, "one", true, "ones", true ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindDeletedNodeInPrefixSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( strOne );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "on" )) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindNodeWithRemovedLabelInPrefixSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "on" )) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindUpdatedNodeInPrefixSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "on" )) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldFindUpdatedNodeInPrefixSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( strOne, prop, stringValue( "ett" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "et" ) ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOne, node.nodeReference() );
        }
    }

    @Test
    void shouldFindSwappedNodeInPrefixSearch() throws Exception
    {
        // Given
        boolean needsValues = indexProvidesStringValues();
        int label = token.nodeLabel( "Node" );
        int prop = token.propertyKey( "prop" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( NODE_PROP_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( strOne, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.stringPrefix( prop, stringValue( "on" )) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    @Test
    void shouldNotFindDeletedNodeInCompositeIndex() throws Exception
    {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( jackDalton );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Jack" ),
                    IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindNodeWithRemovedLabelInCompositeIndex() throws Exception
    {
        // Given
        boolean needsValues = false;
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( joeDalton, label );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Joe" ),
                    IndexQuery.exact( surname, "Dalton" ) );
            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldNotFindUpdatedNodeInCompositeIndex() throws Exception
    {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( jackDalton, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( jackDalton, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Jack" ),
                    IndexQuery.exact( surname, "Dalton" ) );

            // then
            assertFalse( node.next() );
        }
    }

    @Test
    void shouldFindUpdatedNodeInCompositeIndex() throws Exception
    {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeSetProperty( jackDalton, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( jackDalton, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Jesse" ),
                    IndexQuery.exact( surname, "James" ) );

            // then
            assertTrue( node.next() );
            assertEquals( jackDalton, node.nodeReference() );
        }
    }

    @Test
    void shouldFindSwappedNodeInCompositeIndex() throws Exception
    {
        // Given
        boolean needsValues = false;
        int label = token.nodeLabel( "Person" );
        int firstName = token.propertyKey( "firstname" );
        int surname = token.propertyKey( "surname" );
        IndexReadSession index = read.indexReadSession( schemaRead.indexGetForName( COMPOSITE_INDEX_NAME ) );
        try ( KernelTransaction tx = beginTransaction();
              NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( joeDalton, label );
            tx.dataWrite().nodeAddLabel( strOneNoLabel, label );
            tx.dataWrite().nodeSetProperty( strOneNoLabel, firstName, stringValue( "Jesse" ) );
            tx.dataWrite().nodeSetProperty( strOneNoLabel, surname, stringValue( "James" ) );
            tx.dataRead().nodeIndexSeek( index, node, IndexOrder.NONE, needsValues, IndexQuery.exact( firstName, "Jesse" ),
                    IndexQuery.exact( surname, "James" ) );

            // then
            assertTrue( node.next() );
            assertEquals( strOneNoLabel, node.nodeReference() );
        }
    }

    @Test
    void shouldCountDistinctValues() throws Exception
    {
        // Given
        int label = token.nodeLabel( "Node" );
        int key = token.propertyKey( "prop2" );
        IndexDescriptor index = schemaRead.indexGetForName( NODE_PROP_2_INDEX_NAME );
        int expectedCount = 100;
        Map<Value,Set<Long>> expected = new HashMap<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for ( int i = 0; i < expectedCount; i++ )
            {
                Object value = random.nextBoolean() ? String.valueOf( i % 10 ) : (i % 10);
                long nodeId = write.nodeCreate();
                write.nodeAddLabel( nodeId, label );
                write.nodeSetProperty( nodeId, key, Values.of( value ) );
                expected.computeIfAbsent( Values.of( value ), v -> new HashSet<>() ).add( nodeId );
            }
            tx.commit();
        }

        // then
        try ( KernelTransaction tx = beginTransaction();
                NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            tx.dataRead().nodeIndexDistinctValues( index, node, true );
            long totalCount = 0;
            boolean hasValues = true;
            while ( node.next() )
            {
                long count = node.nodeReference();
                if ( node.hasValue() && node.propertyValue( 0 ) != null )
                {
                    Value value = node.propertyValue( 0 );
                    Set<Long> expectedNodes = expected.remove( value );
                    assertNotNull( expectedNodes );
                    assertEquals( count, expectedNodes.size() );
                }
                else
                {
                    // Some providers just can't serve the values for all types, which makes this test unable to do detailed checks for those values
                    // and the total count
                    hasValues = false;
                }
                totalCount += count;
            }
            if ( hasValues )
            {
                assertTrue( expected.isEmpty(), expected.toString() );
            }
            assertEquals( expectedCount, totalCount );
        }
    }

    @Test
    void shouldCountDistinctButSimilarPointValues() throws Exception
    {
        // given
        IndexDescriptor index = schemaRead.indexGetForName( NODE_PROP_3_INDEX_NAME );

        // when
        Map<Value,Integer> expected = new HashMap<>();
        expected.put( POINT_1, 1 );
        expected.put( POINT_2, 2 );
        try ( KernelTransaction tx = beginTransaction();
                NodeValueIndexCursor node = cursors.allocateNodeValueIndexCursor() )
        {
            tx.dataRead().nodeIndexDistinctValues( index, node, true );

            // then
            while ( node.next() )
            {
                assertTrue( node.hasValue() );
                assertTrue( expected.containsKey( node.propertyValue( 0 ) ) );
                assertEquals( expected.remove( node.propertyValue( 0 ) ).intValue(), toIntExact( node.nodeReference() ) );
            }
            assertTrue( expected.isEmpty() );
        }
    }

    private long nodeWithProp( Transaction tx, Object value )
    {
        return nodeWithProp( tx, "prop", value );
    }

    private long nodeWithProp( Transaction tx, String key, Object value )
    {
        Node node = tx.createNode( label( "Node" ) );
        node.setProperty( key, value );
        return node.getId();
    }

    private long nodeWithWhatever( Transaction tx, Object value )
    {
        Node node = tx.createNode( label( "What" ) );
        node.setProperty( "ever", value );
        return node.getId();
    }

    private long nodeWithNoLabel( Transaction tx, Object value )
    {
        Node node = tx.createNode();
        node.setProperty( "prop", value );
        return node.getId();
    }

    private long person( Transaction tx, String firstName, String surname )
    {
        Node node = tx.createNode( label( "Person" ) );
        node.setProperty( "firstname", firstName );
        node.setProperty( "surname", surname );
        return node.getId();
    }
}
