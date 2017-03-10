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
package org.neo4j.store.label;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.LabelIndex;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class DropOtherLabelIndexesIT
{
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;

    @Rule
    public final RandomRule random = new RandomRule();

    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    /**
     * Flip between different label index providers and assert that the unselected label indexes
     * gets deleted so that the selected index will not miss updates if it wasn't the selected index in all sessions.
     */
    @Test
    public void shouldDropUnselectedLabelIndexes() throws Exception
    {
        // GIVEN
        LabelIndex[] types = LabelIndex.values();
        Set<Node> expectedNodes = new HashSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            // WHEN
            GraphDatabaseService db = db( random.among( types ) );
            Node node = createNode( db );
            expectedNodes.add( node );

            // THEN
            assertNodes( db, expectedNodes );
            db.shutdown();
        }
    }

    private static void assertNodes( GraphDatabaseService db, Set<Node> expectedNodes )
    {
        try ( Transaction tx = db.beginTx();
                ResourceIterator<Node> found = db.findNodes( LABEL ) )
        {
            assertEquals( expectedNodes, asSet( found ) );
            tx.success();
        }
    }

    private static Node createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            tx.success();
            return node;
        }
    }

    private GraphDatabaseService db( LabelIndex index )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory.absolutePath() )
                .setConfig( GraphDatabaseSettings.label_index, index.name() )
                .newGraphDatabase();
    }
}
