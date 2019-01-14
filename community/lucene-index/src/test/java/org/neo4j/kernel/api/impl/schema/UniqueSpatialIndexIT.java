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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;

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
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.PointValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class UniqueSpatialIndexIT
{
    private static final String KEY = "prop";
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;

    @Parameterized.Parameters( name = "{0}" )
    public static GraphDatabaseSettings.SchemaIndex[] schemaIndexes()
    {
        return GraphDatabaseSettings.SchemaIndex.values();
    }

    @Parameterized.Parameter( 0 )
    public GraphDatabaseSettings.SchemaIndex schemaIndex;

    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();
    private GraphDatabaseService db;
    private PointValue point1;
    private PointValue point2;

    @Before
    public void setup()
    {
        Pair<PointValue,PointValue> collidingPoints = SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve( Config.defaults() );
        point1 = collidingPoints.first();
        point2 = collidingPoints.other();
    }

    @After
    public void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve()
    {
        // given
        setupDb( schemaIndex );
        Pair<Long,Long> nodeIds = createUniqueNodes();

        // when
        createUniquenessConstraint();

        // then
        assertBothNodesArePresent( nodeIds );
    }

    @Test
    public void shouldAddPointsThatCollideOnSpaceFillingCurveToUniqueIndexInSameTx()
    {
        // given
        setupDb( schemaIndex );
        createUniquenessConstraint();

        // when
        Pair<Long,Long> nodeIds = createUniqueNodes();

        // then
        assertBothNodesArePresent( nodeIds );
    }

    @Test
    public void shouldThrowWhenPopulatingWithNonUniquePoints()
    {
        // given
        setupDb( schemaIndex );
        createNonUniqueNodes();

        // then
        try
        {
            createUniquenessConstraint();
            fail( "Should have failed" );
        }
        catch ( ConstraintViolationException e )
        {   // good
        }
    }

    @Test
    public void shouldThrowWhenAddingNonUniquePoints()
    {
        // given
        setupDb( schemaIndex );
        createUniquenessConstraint();

        // when
        try
        {
            createNonUniqueNodes();
            fail( "Should have failed" );
        }
        catch ( ConstraintViolationException e )
        {   // good
        }
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
        GraphDatabaseBuilder builder = dbFactory.newEmbeddedDatabaseBuilder( directory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() );
        db = builder.newGraphDatabase();
    }
}
