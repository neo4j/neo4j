/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.metatest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.*;

public class TestImpermanentGraphDatabase
{
    private ImpermanentGraphDatabase db;

    @Before
    public void createDb()
    {
        db = new ImpermanentGraphDatabase();
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void should_keep_data_between_start_and_shutdown()
    {
        createNode();

        assertEquals( "Expected one new node", 1, nodeCount() );
    }

    @Test
    public void should_keep_reference_node()
    {
        createNode();
        assertEquals( "Expected one new node", 1, nodeCount() );
        db.cleanContent( true );
        assertEquals( "node 0, for legacy tests", 1, nodeCount() );
        db.cleanContent( false );
        assertEquals( "node 0, for legacy tests", 0, nodeCount() );
    }

    @Test
    public void data_should_not_survive_shutdown()
    {
        createNode();
        db.shutdown();

        createDb();

        assertEquals( "Should not see anything.", 0, nodeCount() );
    }

    private int nodeCount()
    {
        Transaction transaction = db.beginTx();
        int count = IteratorUtil.count( GlobalGraphOperations.at( db ).getAllNodes() );
        transaction.close();
        return count;
    }

    private void createNode()
    {
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
    }
}
