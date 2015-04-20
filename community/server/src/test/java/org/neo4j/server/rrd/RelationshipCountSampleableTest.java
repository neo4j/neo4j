/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.server.rrd.sampler.RelationshipCountSampleable;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class RelationshipCountSampleableTest
{
    public GraphDatabaseAPI db;
    public RelationshipCountSampleable sampleable;

    @Test
    public void emptyDbHasZeroRelationships()
    {
        assertThat( sampleable.getValue(), is( 0d ) );
    }

    @Test
    public void addANodeAndSampleableGoesUp()
    {
        createARelationship( db );

        assertThat( sampleable.getValue(), is( 1d ) );
    }

    private void createARelationship( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        node1.createRelationshipTo( node2, withName( "friend" ) );
        tx.success();
        tx.finish();
    }

    @Before
    public void setUp() throws Exception
    {
        db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        sampleable = new RelationshipCountSampleable( db.getDependencyResolver().resolveDependency( NeoStoreProvider.class ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.db.shutdown();
    }
}
