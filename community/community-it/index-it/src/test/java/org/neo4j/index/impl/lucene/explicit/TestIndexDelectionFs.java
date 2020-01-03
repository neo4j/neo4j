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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestIndexDelectionFs
{
    private static GraphDatabaseAPI db;

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Before
    public void doBefore()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
    }

    @After
    public void doAfter()
    {
        db.shutdown();
    }

    @Test
    public void indexDeleteShouldDeleteDirectory()
    {
        String indexName = "index";
        String otherIndexName = "other-index";

        File indexBaseDir = new File( testDirectory.databaseDir(), "index" );
        File pathToLuceneIndex = LuceneDataSource.getFileDirectory( indexBaseDir,
                new IndexIdentifier( IndexEntityType.Node, indexName ) );
        File pathToOtherLuceneIndex = LuceneDataSource.getFileDirectory( indexBaseDir,
                new IndexIdentifier( IndexEntityType.Node, otherIndexName ) );

        Index<Node> index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( indexName );
            Index<Node> otherIndex = db.index().forNodes( otherIndexName );
            Node node = db.createNode();
            index.add( node, "someKey", "someValue" );
            otherIndex.add( node, "someKey", "someValue" );
            tx.success();
        }

        // Here "index" and "other-index" indexes should exist

        assertTrue( pathToLuceneIndex.exists() );
        assertTrue( pathToOtherLuceneIndex.exists() );
        try ( Transaction tx = db.beginTx() )
        {
            index.delete();
            assertTrue( pathToLuceneIndex.exists() );
            assertTrue( pathToOtherLuceneIndex.exists() );
            tx.success();
        }

        // Here only "other-index" should exist

        assertFalse( pathToLuceneIndex.exists() );
        assertTrue( pathToOtherLuceneIndex.exists() );
    }
}
