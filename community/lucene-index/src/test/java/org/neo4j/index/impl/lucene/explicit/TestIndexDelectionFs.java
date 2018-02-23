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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIndexDelectionFs
{
    private static GraphDatabaseAPI db;

    @BeforeAll
    public static void doBefore() throws IOException
    {
        File directory = new File( "target/test-data/deletion" );
        FileUtils.deleteRecursively( directory );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( directory );
    }

    @AfterAll
    public static void doAfter()
    {
        db.shutdown();
    }

    @Test
    public void indexDeleteShouldDeleteDirectory()
    {
        String indexName = "index";
        String otherIndexName = "other-index";

        File indexBaseDir = new File( db.getStoreDir(), "index" );
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
