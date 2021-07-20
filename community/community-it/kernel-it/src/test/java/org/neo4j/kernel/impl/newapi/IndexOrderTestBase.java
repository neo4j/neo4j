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

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( OtherThreadExtension.class )
@ExtendWith( RandomExtension.class )
abstract class IndexOrderTestBase<ENTITY_VALUE_INDEX_CURSOR extends Cursor & ValueIndexCursor>
        extends KernelAPIWriteTestBase<WriteTestSupport>
{
    protected static final String DEFAULT_PROPERTY_NAME = "prop";
    protected static final String INDEX_NAME = "myIndex";
    protected static final String COMPOSITE_PROPERTY_1 = "prop1";
    protected static final String COMPOSITE_PROPERTY_2 = "prop2";

    @Inject
    private OtherThread otherThread;
    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldRangeScanInOrderWithTxState( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "hello" ) );
            entityWithProp( tx, "bellow" );
            expected.add( entityWithProp( tx, "schmello" ) );
            expected.add( entityWithProp( tx, "low" ) );
            expected.add( entityWithProp( tx, "trello" ) );
            entityWithProp( tx, "yellow" );
            expected.add( entityWithProp( tx, "loww" ) );
            entityWithProp( tx, "below" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityWithProp( tx, "allow" );
                expected.add( entityWithProp( tx, "now" ) );
                expected.add( entityWithProp( tx, "jello" ) );
                entityWithProp( tx, "willow" );

                PropertyIndexQuery query = PropertyIndexQuery.range( prop, "hello", true, "trello", true );

                entityIndexSeek( tx, index, cursor, constrained( indexOrder, true ), query );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldPrefixScanInOrder( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "bee hive" ) );
            entityWithProp( tx, "a" );
            expected.add( entityWithProp( tx, "become" ) );
            expected.add( entityWithProp( tx, "be" ) );
            expected.add( entityWithProp( tx, "bachelor" ) );
            entityWithProp( tx, "street smart" );
            expected.add( entityWithProp( tx, "builder" ) );
            entityWithProp( tx, "ceasar" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityWithProp( tx, "allow" );
                expected.add( entityWithProp( tx, "bastard" ) );
                expected.add( entityWithProp( tx, "bully" ) );
                entityWithProp( tx, "willow" );

                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix( prop, stringValue( "b" ) );
                entityIndexSeek( tx, index, cursor, constrained( indexOrder, true ), query );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderWithPointsAndSingleNodeAfterwards( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            //NOTE: strings come after points in natural ascending sort order
            expected.add( entityWithProp( tx, "a" ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( entityWithProp( tx, "b" ) );

                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderWithPointsWithinSameTile( IndexOrder indexOrder ) throws Exception
    {
        Config config = Config.defaults();
        IndexSpecificSpaceFillingCurveSettings indexSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );
        SpaceFillingCurve curve = indexSettings.forCrs( WGS84 );

        // given
        // Many random points that all are close enough to each other to belong to the same tile on the space filling curve.
        int nbrOfValues = 10000;
        PointValue origin = Values.pointValue( WGS84, 0.0, 0.0 );
        Long derivedValueForCenterPoint = curve.derivedValueFor( origin.coordinate() );
        double[] centerPoint = curve.centerPointFor( derivedValueForCenterPoint );
        double xWidthMultiplier = curve.getTileWidth( 0, curve.getMaxLevel() ) / 2;
        double yWidthMultiplier = curve.getTileWidth( 1, curve.getMaxLevel() ) / 2;

        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            //NOTE: strings come after points in natural ascending sort order
            expected.add( entityWithProp( tx, "a" ) );
            expected.add( entityWithProp( tx, "b" ) );
            for ( int i = 0; i < nbrOfValues / 8; i++ )
            {
                double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y1 ) ) );
                expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y2 ) ) );
                expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y1 ) ) );
                expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y2 ) ) );
            }

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                for ( int i = 0; i < nbrOfValues / 8; i++ )
                {
                    double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y1 ) ) );
                    expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y2 ) ) );
                    expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y1 ) ) );
                    expected.add( entityWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y2 ) ) );
                }
                expected.add( entityWithProp( tx, "c" ) );
                expected.add( entityWithProp( tx, "d" ) );

                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = { "ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderPointsOnly( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );

                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderWithPointsAndNodesOnBothSides( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            //NOTE: arrays come before points in natural ascending sort order
            expected.add( entityWithProp( tx, new String[]{"a"} ) );
            expected.add( entityWithProp( tx, new String[]{"b"} ) );
            expected.add( entityWithProp( tx, new String[]{"c"} ) );
            //NOTE: strings come after points in natural ascending sort order
            expected.add( entityWithProp( tx, "a" ) );
            expected.add( entityWithProp( tx, "b" ) );
            expected.add( entityWithProp( tx, "c" ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( entityWithProp( tx, new String[]{"d"} ) );
                expected.add( entityWithProp( tx, "d" ) );

                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderWithPointsAndNodesBefore( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            //NOTE: arrays come before points in natural ascending sort order
            expected.add( entityWithProp( tx, new String[]{"a"} ) );
            expected.add( entityWithProp( tx, new String[]{"b"} ) );
            expected.add( entityWithProp( tx, new String[]{"c"} ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( entityWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( entityWithProp( tx, new String[]{"d"} ) );

                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValues( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value[]>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, "a", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "a", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "a", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "a", pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertCompositeResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValuesWithOneGapBetween( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value[]>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertCompositeResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValuesWithTwoGapsBetween( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value[]>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, "b", new String[]{"c"} ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertCompositeResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeCompositeIndexScanInOrderWithPointsAndSingleNodeAfterwards( IndexOrder indexOrder ) throws Exception
    {
        List<Pair<Long,Value[]>> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "b" ) );
            expected.add( entityWithTwoProps( tx, "a", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( entityWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                entityIndexScan( tx, index, cursor, constrained( indexOrder, true ) );

                assertCompositeResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldNodeIndexScanInOrderWithStringInMemoryAndConcurrentUpdate( IndexOrder indexOrder ) throws Exception
    {
        String a = "a";
        String b = "b";
        String c = "c";

        createIndex();

        TextValue expectedFirst = indexOrder == IndexOrder.ASCENDING ? stringValue( a ) : stringValue( c );
        TextValue expectedLast = indexOrder == IndexOrder.ASCENDING ? stringValue( c ) : stringValue( a );
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            entityWithProp( tx, a );
            entityWithProp( tx, c );

            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );

            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {

                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix( prop, stringValue( "" ) );
                entityIndexSeek( tx, index, cursor, constrained( indexOrder, true ), query );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ) ).isEqualTo( expectedFirst );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ) ).isEqualTo( expectedLast );

                concurrentInsert( b );

                assertFalse( cursor.next(), () -> "Did not expect to find anything more but found " + cursor.propertyValue( 0 ) );
            }
            tx.commit();
        }

        // Verify we see all data in the end
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( INDEX_NAME ) );
            try ( var cursor = getEntityValueIndexCursor( tx ) )
            {
                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix( prop, stringValue( "" ) );
                entityIndexSeek( tx, index, cursor, constrained( indexOrder, true ), query );
                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ) ).isEqualTo( expectedFirst );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ) ).isEqualTo( stringValue( b ) );

                assertTrue( cursor.next() );
                assertThat( cursor.propertyValue( 0 ) ).isEqualTo( expectedLast );

                assertFalse( cursor.next() );
            }
        }
    }

    private void concurrentInsert( Object value ) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        otherThread.execute( () ->
        {
            try ( KernelTransaction otherTx = beginTransaction() )
            {
                entityWithProp( otherTx, value );
                otherTx.commit();
            }
            return null;
        } ).get();
    }

    protected void assertResultsInOrder( List<Pair<Long,Value>> expected, ENTITY_VALUE_INDEX_CURSOR cursor, IndexOrder indexOrder )
    {
        Comparator<Pair<Long,Value>> comparator = indexOrder == IndexOrder.ASCENDING ? ( a, b ) -> Values.COMPARATOR.compare( a.other(), b.other() )
                                                                                     : ( a, b ) -> Values.COMPARATOR.compare( b.other(), a.other() );

        expected.sort( comparator );
        Iterator<Pair<Long,Value>> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            Pair<Long,Value> expectedRow = expectedRows.next();
            assertThat( entityReference( cursor ) )
                    .as( expectedRow.other() + " == " + cursor.propertyValue( 0 ) )
                    .isEqualTo( expectedRow.first() );
            for ( int i = 0; i < cursor.numberOfProperties(); i++ )
            {
                Value value = cursor.propertyValue( i );
                assertThat( value ).isEqualTo( expectedRow.other() );
            }
        }

        assertFalse( expectedRows.hasNext() );
        assertFalse( cursor.next() );
    }

    private void assertCompositeResultsInOrder( List<Pair<Long,Value[]>> expected, ENTITY_VALUE_INDEX_CURSOR cursor, IndexOrder indexOrder )
    {
        Comparator<Pair<Long,Value[]>> comparator = indexOrder ==
                                                    IndexOrder.ASCENDING ?
                                                    ( a, b ) ->
                                                    {
                                                        int compare = Values.COMPARATOR.compare( a.other()[0], b.other()[0] );
                                                        if ( compare == 0 )
                                                        {
                                                            return Values.COMPARATOR.compare( a.other()[1], b.other()[1] );
                                                        }
                                                        return compare;
                                                    }
                                                                         :
                                                    ( a, b ) ->
                                                    {
                                                        int compare = -Values.COMPARATOR.compare( a.other()[0], b.other()[0] );
                                                        if ( compare == 0 )
                                                        {
                                                            return -Values.COMPARATOR.compare( a.other()[1], b.other()[1] );
                                                        }
                                                        return compare;
                                                    };

        expected.sort( comparator );
        Iterator<Pair<Long,Value[]>> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            Pair<Long,Value[]> expectedRow = expectedRows.next();
            assertThat( entityReference( cursor ) )
                    .as( expectedRow.other()[0] + " == " + cursor.propertyValue( 0 ) + " && " + expectedRow.other()[1] + " == " + cursor.propertyValue( 1  ))
                    .isEqualTo( expectedRow.first() );
            for ( int i = 0; i < cursor.numberOfProperties(); i++ )
            {
                Value value = cursor.propertyValue( i );
                assertThat( value ).isEqualTo( expectedRow.other()[i] );
            }
        }

        assertFalse( expectedRows.hasNext() );
        assertFalse( cursor.next() );
    }

    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport();
    }

    protected abstract void createIndex();

    protected abstract void createCompositeIndex();

    protected abstract long entityReference( ENTITY_VALUE_INDEX_CURSOR cursor );

    protected abstract Pair<Long,Value> entityWithProp( KernelTransaction tx, Object value ) throws Exception;

    protected abstract Pair<Long,Value[]> entityWithTwoProps( KernelTransaction tx, Object value1, Object value2 ) throws Exception;

    protected abstract ENTITY_VALUE_INDEX_CURSOR getEntityValueIndexCursor( KernelTransaction tx );

    protected abstract void entityIndexScan( KernelTransaction tx, IndexReadSession index, ENTITY_VALUE_INDEX_CURSOR cursor,
                                             IndexQueryConstraints constraints ) throws KernelException;

    protected abstract void entityIndexSeek( KernelTransaction tx, IndexReadSession index, ENTITY_VALUE_INDEX_CURSOR cursor,
                                             IndexQueryConstraints constraints, PropertyIndexQuery query ) throws KernelException;

}
