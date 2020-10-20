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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.RandomRule;
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
public abstract class NodeIndexOrderTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    private final String indexName = "myIndex";
    @Inject
    private OtherThreadRule otherThreadRule;
    @Inject
    private RandomRule random;

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
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
            expected.add( nodeWithProp( tx, "loww" ) );
            nodeWithProp( tx, "below" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "now" ) );
                expected.add( nodeWithProp( tx, "jello" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.range( prop, "hello", true, "trello", true );
                tx.dataRead().nodeIndexSeek( index, cursor, constrained( indexOrder, true ), query );

                assertResultsInOrder( expected, cursor, indexOrder );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class, names = {"ASCENDING", "DESCENDING"} )
    void shouldLabelScanInOrder( IndexOrder indexOrder ) throws Exception
    {
        List<Long> expected = new ArrayList<>();

        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            nodeWithLabel( tx, "OUTSIDE1" );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            nodeWithLabel( tx, "OUTSIDE2" );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            nodeWithLabel( tx, "OUTSIDE1" );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            nodeWithLabel( tx, "OUTSIDE1" );
            nodeWithLabel( tx, "OUTSIDE1" );
            nodeWithLabel( tx, "OUTSIDE2" );
            expected.add( nodeWithLabel( tx, "INSIDE" ) );
            nodeWithLabel( tx, "OUTSIDE2" );
            tx.commit();
        }

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "INSIDE" );
            tx.dataRead().prepareForLabelScans();

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( tx.pageCursorTracer() ) )
            {
                nodeWithLabel( tx, "OUTSIDE1" );
                nodeWithLabel( tx, "OUTSIDE1" );
                expected.add( nodeWithLabel( tx, "INSIDE" ) );
                nodeWithLabel( tx, "OUTSIDE1" );
                nodeWithLabel( tx, "OUTSIDE2" );
                expected.add( nodeWithLabel( tx, "INSIDE" ) );
                expected.add( nodeWithLabel( tx, "INSIDE" ) );
                expected.add( nodeWithLabel( tx, "INSIDE" ) );
                nodeWithLabel( tx, "OUTSIDE2" );
                expected.add( nodeWithLabel( tx, "INSIDE" ) );

                tx.dataRead().nodeLabelScan( label, cursor, indexOrder );

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

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                nodeWithProp( tx, "allow" );
                expected.add( nodeWithProp( tx, "bastard" ) );
                expected.add( nodeWithProp( tx, "bully" ) );
                nodeWithProp( tx, "willow" );

                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "b" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, constrained( indexOrder, true ), query );

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
            expected.add( nodeWithProp( tx, "a" ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, "b" ) );

                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithProp( tx, "a" ) );
            expected.add( nodeWithProp( tx, "b" ) );
            for ( int i = 0; i < nbrOfValues / 8; i++ )
            {
                double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y1 ) ) );
                expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y2 ) ) );
                expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y1 ) ) );
                expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y2 ) ) );
            }

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                for ( int i = 0; i < nbrOfValues / 8; i++ )
                {
                    double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y1 ) ) );
                    expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x1, centerPoint[1] + y2 ) ) );
                    expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y1 ) ) );
                    expected.add( nodeWithProp( tx, Values.pointValue( WGS84, centerPoint[0] + x2, centerPoint[1] + y2 ) ) );
                }
                expected.add( nodeWithProp( tx, "c" ) );
                expected.add( nodeWithProp( tx, "d" ) );

                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );

                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithProp( tx, new String[]{"a"} ) );
            expected.add( nodeWithProp( tx, new String[]{"b"} ) );
            expected.add( nodeWithProp( tx, new String[]{"c"} ) );
            //NOTE: strings come after points in natural ascending sort order
            expected.add( nodeWithProp( tx, "a" ) );
            expected.add( nodeWithProp( tx, "b" ) );
            expected.add( nodeWithProp( tx, "c" ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, new String[] {"d"} ) );
                expected.add( nodeWithProp( tx, "d" ) );

                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithProp( tx, new String[]{"a"} ) );
            expected.add( nodeWithProp( tx, new String[]{"b"} ) );
            expected.add( nodeWithProp( tx, new String[]{"c"} ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithProp( tx, pointValue( Cartesian, 500000, 500000 ) ) );

            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, -400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, -400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, pointValue( Cartesian, 400000, 400000 ) ) );
                expected.add( nodeWithProp( tx, new String[] {"d"} ) );

                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ));
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "b" ));
            expected.add( nodeWithTwoProps( tx, "a",  pointValue( Cartesian, -500000, -500000 ) ));
            expected.add( nodeWithTwoProps( tx, "a",  pointValue( Cartesian, 500000, -500000 ) ));
            expected.add( nodeWithTwoProps( tx, "a",  pointValue( Cartesian, -500000, 500000 ) ));
            expected.add( nodeWithTwoProps( tx, "a",  pointValue( Cartesian, 500000, 500000 ) ));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ));
            expected.add( nodeWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ));
            expected.add( nodeWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, "b", new String[]{"c"} ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            expected.add( nodeWithTwoProps( tx, new String[]{"a"}, new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "a" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "a" ));
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, -500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, -500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, -500000, 500000 ), "b" ) );
            expected.add( nodeWithTwoProps( tx, pointValue( Cartesian, 500000, 500000 ), "b" ));
            expected.add( nodeWithTwoProps( tx, "a",  pointValue( Cartesian, 500000, 500000 ) ));
            expected.add( nodeWithTwoProps( tx, "b", new String[]{"b"} ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, -500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, -500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "b", pointValue( Cartesian, 500000, 500000 ) ) );
            expected.add( nodeWithTwoProps( tx, "c", new String[]{"b"} ) );

            tx.commit();
        }

        createCompositeIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                tx.dataRead().nodeIndexScan( index, cursor, constrained( indexOrder, true ) );

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
            int prop = tx.tokenRead().propertyKey( "prop" );
            nodeWithProp( tx, a );
            nodeWithProp( tx, c );

            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );

            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {

                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, constrained( indexOrder, true ), query );

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
            int prop = tx.tokenRead().propertyKey( "prop" );
            IndexReadSession index = tx.dataRead().indexReadSession( tx.schemaRead().indexGetForName( indexName ) );
            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( tx.pageCursorTracer(), tx.memoryTracker() ) )
            {
                IndexQuery query = IndexQuery.stringPrefix( prop, stringValue( "" ) );
                tx.dataRead().nodeIndexSeek( index, cursor, constrained( indexOrder, true ), query );
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
        otherThreadRule.execute( () ->
        {
            try ( KernelTransaction otherTx = beginTransaction() )
            {
                nodeWithProp( otherTx, value );
                otherTx.commit();
            }
            return null;
        } ).get();
    }

    private void assertResultsInOrder( List<Pair<Long,Value>> expected, NodeValueIndexCursor cursor, IndexOrder indexOrder )
    {
        Comparator<Pair<Long,Value>> comparator = indexOrder == IndexOrder.ASCENDING ? ( a, b ) -> Values.COMPARATOR.compare( a.other(), b.other() )
                                                                                     : ( a, b ) -> Values.COMPARATOR.compare( b.other(), a.other() );

        expected.sort( comparator );
        Iterator<Pair<Long,Value>> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            Pair<Long,Value> expectedRow = expectedRows.next();
            assertThat( cursor.nodeReference() )
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

    private void assertCompositeResultsInOrder( List<Pair<Long,Value[]>> expected, NodeValueIndexCursor cursor, IndexOrder indexOrder )
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
            assertThat( cursor.nodeReference() )
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

    private void assertResultsInOrder( List<Long> expected, NodeLabelIndexCursor cursor, IndexOrder indexOrder )
    {

        Comparator<Long> c = indexOrder == IndexOrder.ASCENDING ? Comparator.naturalOrder() : Comparator.reverseOrder();
        expected.sort( c );
        Iterator<Long> expectedRows = expected.iterator();
        while ( cursor.next() && expectedRows.hasNext() )
        {
            long expectedRow = expectedRows.next();
            assertThat( cursor.nodeReference() ).isEqualTo( expectedRow );
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
            tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
        }
    }

    private void createCompositeIndex()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Node" ) ).on( "prop1" ).on( "prop2" ).withName( indexName ).create();
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
        }
    }

    private long nodeWithLabel( KernelTransaction tx, String label ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( label ) );
        return node;
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

    private Pair<Long,Value[]> nodeWithTwoProps( KernelTransaction tx, Object value1, Object value2 ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        TokenWrite tokenWrite = tx.tokenWrite();
        write.nodeAddLabel( node, tokenWrite.labelGetOrCreateForName( "Node" ) );
        Value val1 = Values.of( value1 );
        Value val2 = Values.of( value2 );
        write.nodeSetProperty( node, tokenWrite.propertyKeyGetOrCreateForName( "prop1" ), val1 );
        write.nodeSetProperty( node, tokenWrite.propertyKeyGetOrCreateForName( "prop2" ), val2 );
        return Pair.of( node, new Value[]{val1, val2} );
    }
}
