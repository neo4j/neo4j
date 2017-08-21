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
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
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
    public DatabaseRule dbRule = new EmbeddedDatabaseRule();

    private static final Label LABEL = Label.label( "label" );
    private static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop", "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                        "between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                assertEquals( firstID, reader.query( "hello" ).next() );
                assertEquals( secondID, reader.query( "zebra" ).next() );
                assertEquals( secondID, reader.query( "zedonk" ).next() );
                assertEquals( secondID, reader.query( "cross" ).next() );
            }
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop", "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                        "between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( firstID );
                node.setProperty( "prop", "Hahahaha! potato!" );
                Node node2 = db.getNodeById( secondID );
                node2.setProperty( "prop", "This one is a potato farmer." );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                assertFalse( reader.query( "hello" ).hasNext() );
                assertFalse( reader.query( "zebra" ).hasNext() );
                assertFalse( reader.query( "zedonk" ).hasNext() );
                assertFalse( reader.query( "cross" ).hasNext() );
                assertEquals( firstID, reader.query( "hahahaha" ).next() );
                assertEquals( secondID, reader.query( "farmer" ).next() );
                PrimitiveLongIterator iterator = reader.query( "potato" );
                assertEquals( firstID, iterator.next() );
                assertEquals( secondID, iterator.next() );
            }
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop", "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                        "between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( firstID ).delete();
                db.getNodeById( secondID ).delete();

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                assertFalse( reader.query( "hello" ).hasNext() );
                assertFalse( reader.query( "zebra" ).hasNext() );
                assertFalse( reader.query( "zedonk" ).hasNext() );
                assertFalse( reader.query( "cross" ).hasNext() );
            }
        }
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node.setProperty( "prop2", "zebra" );
                Node node2 = db.createNode( LABEL );
                node2.setProperty( "prop2", "zebra" );
                node2.setProperty( "prop3", "hello" );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                PrimitiveLongIterator hello = reader.query( "hello" );
                assertEquals( firstID, hello.next() );
                assertFalse( hello.hasNext() );
                assertFalse( reader.query( "zebra" ).hasNext() );
            }
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
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
                node2.setProperty( "prop", "Olof och Hans tomtar med karl som tomtar ofta" );
                node2.setProperty( "prop2", "karl tomtar" );
                Node node3 = db.createNode( LABEL );
                thirdID = node3.getId();
                node3.setProperty( "prop", "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                PrimitiveLongIterator iterator = reader.query( "tomtar", "karl" );
                assertEquals( secondID, iterator.next() );
                assertEquals( thirdID, iterator.next() );
                assertEquals( firstID, iterator.next() );
            }
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "first", "last" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "first", "Full" );
                node.setProperty( "last", "Hanks" );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "first", "Tom" );
                node2.setProperty( "last", "Hunk" );
                Node node3 = db.createNode( LABEL );
                thirdID = node3.getId();
                node3.setProperty( "first", "Tom" );
                node3.setProperty( "last", "Hanks" );
                Node node4 = db.createNode( LABEL );
                fourthID = node4.getId();
                node4.setProperty( "first", "Tom Hanks" );
                node4.setProperty( "last", "Tom Hanks" );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                PrimitiveLongIterator iterator = reader.query( "Tom", "Hanks" );
                assertEquals( fourthID, iterator.next() );
                assertEquals( thirdID, iterator.next() );
                assertEquals( firstID, iterator.next() );
                assertEquals( secondID, iterator.next() );
            }
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        try ( InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), "prop" ) )
        {
            db.registerTransactionEventHandler( insightIndex.getUpdater() );

            long firstNodeID;
            long secondNodeID;
            long firstRelID;
            long secondRelID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstNodeID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondNodeID = node2.getId();
                node2.setProperty( "prop", "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                        "between a zebra and any other equine: essentially, a zebra hybrid." );
                Relationship rel1 = node.createRelationshipTo( node2, RELTYPE );
                firstRelID = rel1.getId();
                rel1.setProperty( "prop", "Hello. Hello again." );
                Relationship rel2 = node2.createRelationshipTo( node, RELTYPE );
                secondRelID = rel2.getId();
                rel2.setProperty( "prop", "And now, something completely different" );

                tx.success();
            }

            try ( InsightIndexReader reader = insightIndex.getNodeReader() )
            {

                PrimitiveLongIterator hello = reader.query( "hello" );
                assertEquals( firstNodeID, hello.next() );
                assertFalse( hello.hasNext() );
                PrimitiveLongIterator zebra = reader.query( "zebra" );
                assertEquals( secondNodeID, zebra.next() );
                assertFalse( zebra.hasNext() );
                PrimitiveLongIterator different = reader.query( "different" );
                assertFalse( different.hasNext() );
            }
            try ( InsightIndexReader reader = insightIndex.getRelationshipReader() )
            {

                PrimitiveLongIterator hello = reader.query( "hello" );
                assertEquals( firstRelID, hello.next() );
                assertFalse( hello.hasNext() );
                PrimitiveLongIterator zebra = reader.query( "zebra" );
                assertFalse( zebra.hasNext() );
                PrimitiveLongIterator different = reader.query( "different" );
                assertEquals( secondRelID, different.next() );
                assertFalse( different.hasNext() );
            }
        }
    }
}
