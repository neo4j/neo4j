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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestIndexImplOnNeo
{
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private GraphDatabaseService db;

    @Before
    public void createDb() throws Exception
    {
        db = new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase( new File( "mydb" ) );
    }

    private void restartDb() throws Exception
    {
        shutdownDb();
        createDb();
    }

    @After
    public void shutdownDb() throws Exception
    {
        db.shutdown();
    }

    @Test
    public void createIndexWithProviderThatUsesNeoAsDataSource() throws Exception
    {
        String indexName = "inneo";
        assertFalse( indexExists( indexName ) );
        Map<String, String> config = stringMap( PROVIDER, "test-dummy-neo-index",
                "config1", "A value", "another config", "Another value" );

        Index<Node> index;
        try ( Transaction transaction = db.beginTx() )
        {
            index = db.index().forNodes( indexName, config );
            transaction.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( indexExists( indexName ) );
            assertEquals( config, db.index().getConfiguration( index ) );
            assertEquals( 0, count( (Iterable<Node>) index.get( "key", "something else" ) ) );
            tx.success();
        }

        restartDb();

        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( indexExists( indexName ) );
            assertEquals( config, db.index().getConfiguration( index ) );
            tx.success();
        }
    }

    private boolean indexExists( String indexName )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            boolean exists = db.index().existsForNodes( indexName );
            transaction.success();
            return exists;
        }
    }
}
