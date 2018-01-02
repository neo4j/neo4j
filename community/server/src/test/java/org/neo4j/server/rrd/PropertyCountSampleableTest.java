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
package org.neo4j.server.rrd;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.server.rrd.sampler.PropertyCountSampleable;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PropertyCountSampleableTest
{
    @Rule
    public final DatabaseRule dbRule = new ImpermanentDatabaseRule();
    public PropertyCountSampleable sampleable;

    @Test
    public void emptyDbHasZeroNodesInUse()
    {
        assertThat( sampleable.getValue(), is( 0d ) );
    }

    @Test
    public void addANodeWithPropertyAndSampleableGoesUp()
    {
        createNodeWithProperty();
        assertThat( sampleable.getValue(), is( 1d ) );
    }

    @Test
    public void addOnlyPropertiesAndSampleableGoesUp()
    {
        long nodeId = createNodeWithProperty();
        assertThat( sampleable.getValue(), is( 1d ) );

        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "id", UUID.randomUUID().toString() );
            tx.success();
        }

        assertThat( sampleable.getValue(), is( 2d ) );
    }

    @Before
    public void setupReferenceNode()
    {
        DependencyResolver dependencyResolver = dbRule.getGraphDatabaseAPI().getDependencyResolver();
        NeoStoresSupplier neoStore = dependencyResolver.resolveDependency( NeoStoresSupplier.class );
        AvailabilityGuard gaurd = dependencyResolver.resolveDependency( AvailabilityGuard.class );
        sampleable = new PropertyCountSampleable( neoStore, gaurd );
    }

    private long createNodeWithProperty()
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            Node n = db.createNode();
            n.setProperty( "monkey", "rock!" );
            tx.success();
            return n.getId();
        }
    }
}
