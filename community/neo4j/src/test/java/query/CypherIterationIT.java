/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package query;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.api.state.TransactionStatesContainer;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static org.junit.Assert.assertEquals;

public class CypherIterationIT
{
    @Rule
    public final DatabaseRule database = new EmbeddedDatabaseRule();
    private final int NODES = 500_0;

    @Before
    public void setUp() throws Exception
    {
        FeatureToggles.set( TransactionStatesContainer.class, "multiState", true );
    }

    @After
    public void tearDown() throws Exception
    {
        FeatureToggles.clear( TransactionStatesContainer.class, "multiState" );
    }

    @Test
    public void createEmptyNodeFromMatchAll()
    {
        createFiveNodes();

        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "MATCH(n) CREATE()" );
            transaction.success();
        }

        countNodes( 2 * NODES );
    }

    @Test
    public void createLabeledNodesFromMatchAll()
    {
        createFiveNodes();

        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "MATCH(n) CREATE(m:HUMAN:OPERATOR)" );
            transaction.success();
        }

        countNodes( 2 * NODES );
        countNodesWithLabel( Label.label( "HUMAN" ), NODES );
        countNodesWithLabel( Label.label( "OPERATOR" ), NODES );
    }

    @Test
    public void createNodesWithPropertisFromMatchAll()
    {
        createFiveNodes();

        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "MATCH(n) CREATE(m:ROBOT {name: 'Bender'})" );
            transaction.success();
        }

        countNodes( 2 * NODES );
        countNodesWithLabelAndPropertyValue( Label.label( "ROBOT" ), "name", "Bender", NODES );
    }

    private void createFiveNodes()
    {
        int step = 1000;
        for ( int created = 0; created < NODES; created += step )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                for ( int j = 0; j < step; j++ )
                {
                    database.createNode();
                }
                transaction.success();
            }
        }
    }

    private void countNodes( int expectedNodes )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( expectedNodes, Iterables.count( database.getAllNodes() ) );
            transaction.success();
        }
    }

    private void countNodesWithLabel( Label label, int expectedNodes )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( expectedNodes, Iterators.count( database.findNodes( label ) ) );
            transaction.success();
        }
    }

    private void countNodesWithLabelAndPropertyValue( Label label, String propertyName, Object propertyValue, int expectedNodes )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( expectedNodes, Iterators.count( database.findNodes( label, propertyName, propertyValue ) ) );
            transaction.success();
        }
    }
}
