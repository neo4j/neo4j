/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.server.rrd.sampler.NodeIdsInUseSampleable;
import org.neo4j.test.ImpermanentGraphDatabase;

public class NodeIdsInUseSampleableTest
{
    public Database db;
    public NodeIdsInUseSampleable sampleable;

    @Test
    public void emptyDbHasZeroNodesInUse()
    {
        // Reference node is always created in empty dbs
        assertThat( sampleable.getValue(), is( 1d ) );
    }

    @Test
    public void addANodeAndSampleableGoesUp()
    {
        double oldValue = sampleable.getValue();

        createNode( db.getGraph() );

        assertThat( sampleable.getValue(), greaterThan( oldValue ) );
    }

    private void createNode( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
    }

    @Before
    public void setUp() throws Exception
    {
        db = new WrappingDatabase( new ImpermanentGraphDatabase() );
        sampleable = new NodeIdsInUseSampleable( db.getGraph() );
    }

    @After
    public void shutdown() throws Throwable
    {
        db.getGraph().shutdown();
    }
}
