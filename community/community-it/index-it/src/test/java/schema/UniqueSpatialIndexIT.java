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
package schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexValueTestUtil;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.PointValue;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class UniqueSpatialIndexIT
{
    private static final String KEY = "prop";
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;

    @Inject
    private TestDirectory directory;
    private GraphDatabaseService db;
    private PointValue point1;
    private PointValue point2;

    @BeforeEach
    void setup()
    {
        Pair<PointValue,PointValue> collidingPoints = SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve( Config.defaults() );
        point1 = collidingPoints.first();
        point2 = collidingPoints.other();
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "providerSettings" )
    void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        // given
        setupDb( schemaIndex );
        Pair<Long,Long> nodeIds = createUniqueNodes();

        // when
        createUniquenessConstraint();

        // then
        assertBothNodesArePresent( nodeIds );
    }

    @ParameterizedTest
    @MethodSource( "providerSettings" )
    void shouldAddPointsThatCollideOnSpaceFillingCurveToUniqueIndexInSameTx( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        // given
        setupDb( schemaIndex );
        createUniquenessConstraint();

        // when
        Pair<Long,Long> nodeIds = createUniqueNodes();

        // then
        assertBothNodesArePresent( nodeIds );
    }

    @ParameterizedTest
    @MethodSource( "providerSettings" )
    void shouldThrowWhenPopulatingWithNonUniquePoints( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        // given
        setupDb( schemaIndex );
        createNonUniqueNodes();

        // then
        assertThrows( ConstraintViolationException.class, this::createUniquenessConstraint );
    }

    @ParameterizedTest
    @MethodSource( "providerSettings" )
    void shouldThrowWhenAddingNonUniquePoints( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        // given
        setupDb( schemaIndex );
        createUniquenessConstraint();

        // when
        assertThrows( ConstraintViolationException.class, this::createNonUniqueNodes );
    }

    private static Stream<GraphDatabaseSettings.SchemaIndex> providerSettings()
    {
        return Arrays.stream( GraphDatabaseSettings.SchemaIndex.values() );
    }

    private void createNonUniqueNodes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node originNode = db.createNode( LABEL );
            originNode.setProperty( KEY, point1 );
            Node centerNode = db.createNode( LABEL );
            centerNode.setProperty( KEY, point1 );
            tx.success();
        }
    }

    private Pair<Long,Long> createUniqueNodes()
    {
        Pair<Long,Long> nodeIds;
        try ( Transaction tx = db.beginTx() )
        {
            Node originNode = db.createNode( LABEL );
            originNode.setProperty( KEY, point1 );
            Node centerNode = db.createNode( LABEL );
            centerNode.setProperty( KEY, point2 );

            nodeIds = Pair.of( originNode.getId(), centerNode.getId() );
            tx.success();
        }
        return nodeIds;
    }

    private void assertBothNodesArePresent( Pair<Long,Long> nodeIds )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> origin = db.findNodes( LABEL, KEY, point1 );
            assertTrue( origin.hasNext() );
            assertEquals( nodeIds.first().longValue(), origin.next().getId() );
            assertFalse( origin.hasNext() );

            ResourceIterator<Node> center = db.findNodes( LABEL, KEY, point2 );
            assertTrue( center.hasNext() );
            assertEquals( nodeIds.other().longValue(), center.next().getId() );
            assertFalse( center.hasNext() );

            tx.success();
        }
    }

    private void createUniquenessConstraint()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( TestLabels.LABEL_ONE ).assertPropertyIsUnique( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private void setupDb( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = dbFactory.newEmbeddedDatabaseBuilder( directory.storeDir() )
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() );
        db = builder.newGraphDatabase();
    }
}
