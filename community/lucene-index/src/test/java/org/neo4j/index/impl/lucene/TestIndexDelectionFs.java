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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.FileUtils;

import static org.junit.Assert.*;

public class TestIndexDelectionFs
{
    private static GraphDatabaseService db;
    
    @BeforeClass
    public static void doBefore() throws IOException
    {
        FileUtils.deleteRecursively( new File( "target/test-data/deletion" ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabase( "target/test-data/deletion" );
    }
    
    @AfterClass
    public static void doAfter()
    {
        db.shutdown();
    }
    
    @Test
    public void indexDeleteShouldDeleteDirectory()
    {
        String indexName = "index";
        String otherIndexName = "other-index";

        StringBuffer tempPath = new StringBuffer( ((GraphDatabaseAPI)db).getStoreDir())
                .append(File.separator).append("index").append(File.separator)
                .append("lucene").append(File.separator).append("node")
                .append(File.separator);

        File pathToLuceneIndex = new File( tempPath.toString() + indexName );
        File pathToOtherLuceneIndex = new File( tempPath.toString() + otherIndexName );

        Transaction tx = db.beginTx();
        Index<Node> index;
        try
        {
            index = db.index().forNodes( indexName );
            Index<Node> otherIndex = db.index().forNodes( otherIndexName );
            Node node = db.createNode();
            index.add( node, "someKey", "someValue" );
            otherIndex.add( node, "someKey", "someValue" );
            assertFalse( pathToLuceneIndex.exists() );
            assertFalse( pathToOtherLuceneIndex.exists() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Here "index" and "other-index" indexes should exist

        assertTrue( pathToLuceneIndex.exists() );
        assertTrue( pathToOtherLuceneIndex.exists() );
        tx = db.beginTx();
        try
        {
            index.delete();
            assertTrue( pathToLuceneIndex.exists() );
            assertTrue( pathToOtherLuceneIndex.exists() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Here only "other-index" should exist

        assertFalse( pathToLuceneIndex.exists() );
        assertTrue( pathToOtherLuceneIndex.exists() );
    }
}
