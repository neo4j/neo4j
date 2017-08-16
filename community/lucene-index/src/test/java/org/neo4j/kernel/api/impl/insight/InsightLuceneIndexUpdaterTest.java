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
package org.neo4j.kernel.api.impl.insight;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class InsightLuceneIndexUpdaterTest
{
    @ClassRule
    public static FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule( testDirectory.graphDbDir() );

    private static final Label LABEL = Label.label( "label1" );

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler(insightIndex.getUpdater());

            long firstID;
            long secondID;
            try (Transaction tx = db.beginTx()) {
                Node node = db.createNode(LABEL);
                firstID = node.getId();
                node.setProperty("prop", "Hello. Hello again.");
                Node node2 = db.createNode(LABEL);
                secondID = node2.getId();
                node2.setProperty("prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                                "between a zebra and any other equine: essentially, a zebra hybrid.");

                tx.success();
            }

            try (InsightIndexReader reader = insightIndex.getReader()) {

                assertEquals(firstID, reader.query("hello").next());
                assertEquals(secondID, reader.query("zebra").next());
                assertEquals(secondID, reader.query("zedonk").next());
                assertEquals(secondID, reader.query("cross").next());
            }
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler(insightIndex.getUpdater());

            long firstID;
            long secondID;
            try (Transaction tx = db.beginTx()) {
                Node node = db.createNode(LABEL);
                firstID = node.getId();
                node.setProperty("prop", "Hello. Hello again.");
                Node node2 = db.createNode(LABEL);
                secondID = node2.getId();
                node2.setProperty("prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                                "between a zebra and any other equine: essentially, a zebra hybrid.");

                tx.success();
            }

            try (Transaction tx = db.beginTx()) {
                db.getNodeById(firstID).delete();
                db.getNodeById(secondID).delete();

                tx.success();
            }

            try (InsightIndexReader reader = insightIndex.getReader()) {

                assertFalse(reader.query("hello").hasNext());
                assertFalse(reader.query("zebra").hasNext());
                assertFalse(reader.query("zedonk").hasNext());
                assertFalse(reader.query("cross").hasNext());
            }
        }
    }

    @Test
    public void shouldOrderResults() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop", "prop2" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            long thirdID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Tomtar tomtar oftsat i tomteutstyrsel." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop", "tomtar tomtar tomtar tomtar tomtar." );
                node2.setProperty( "prop2", "tomtar tomtar tomtar tomtar tomtar tomtar karl" );
                Node node3 = db.createNode( LABEL );
                thirdID = node3.getId();
                node3.setProperty( "prop", "Tomtar som tomtar ser upp till tomtar som inte tomtar." );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getReader() )
            {

                PrimitiveLongIterator iterator = reader.query( "tomtar", "karl" );
                assertEquals( secondID, iterator.next() );
                assertEquals( firstID, iterator.next() );
                assertEquals( thirdID, iterator.next() );
            }
        }
    }
}
